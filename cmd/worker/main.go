package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"nexus/nexusconfig"
	"os" // <-- TAMBAHKAN IMPORT "os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv" // <-- TAMBAHKAN IMPORT "godotenv"
)

// (Deklarasi var global tetap sama)
var (
	rdb             *redis.Client
	nexusDB         *sql.DB
	cfg             *nexusconfig.Config
	targetDBPool    = make(map[string]*sql.DB)
	poolMutex       = &sync.Mutex{}
	decryptionCache = make(map[string]string)
	decryptMutex    = &sync.RWMutex{}
	httpClient      = &http.Client{Timeout: 30 * time.Second}
)

// (Struct IntegrationJob dan FieldMapping tetap sama)
type FieldMapping struct {
	SourceField string
	TargetField string
}
type IntegrationJob struct {
	SubscriptionID  int
	TargetTable     string
	Driver          string
	TargetDSNFormat string
	EncryptedPass   string
	Mappings        []FieldMapping
}

func main() {
	var err error

	// --- PERUBAHAN DI SINI ---
	// 1. Muat file .env SECARA MANUAL di paling atas
	if err := godotenv.Load(); err != nil {
		log.Fatal("KRITIS: Gagal memuat file .env. Pastikan .env ada di root.")
	}
	log.Println("File .env berhasil dimuat.")
	// -------------------------

	// 2. Muat config.yml (sekarang hanya untuk config non-rahasia)
	cfg, err = nexusconfig.LoadConfig()
	if err != nil {
		log.Fatalf("Gagal memuat config: %v", err)
	}

	// (Sisa fungsi main() tetap sama)
	rdb = redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
	})
	// ...
	// (Lanjutkan sisa fungsi main() seperti sebelumnya)
	// ...
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Gagal koneksi ke Redis: %v", err)
	}

	nexusDB, err = sql.Open("mysql", cfg.Database.NexusDBDSN)
	if err != nil {
		log.Fatalf("Gagal koneksi ke Nexus DB: %v", err)
	}
	nexusDB.SetConnMaxLifetime(time.Minute * 3)
	nexusDB.SetMaxOpenConns(10)
	nexusDB.SetMaxIdleConns(10)

	log.Println("Worker terhubung ke Redis dan Nexus DB.")
	createConsumerGroup()
	log.Println("Menunggu pesan di stream:", cfg.Redis.StreamName)

	for {
		streams, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    cfg.Redis.ConsumerGroup,
			Consumer: "go-worker-1",
			Streams:  []string{cfg.Redis.StreamName, ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()

		if err != nil {
			if err != redis.Nil {
				log.Printf("ERROR: Gagal XReadGroup: %v", err)
			}
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				log.Printf("Menerima pesan ID: %s", message.ID)
				go processMessage(message)
			}
		}
	}
}

// (Fungsi createConsumerGroup, processMessage, executeJob,
// getFanOutJobs, getMappings, transformData, insertData tetap sama)
// ...
// (Salin semua fungsi lain dari kode sebelumnya)
// ...

// (Salin createConsumerGroup)
func createConsumerGroup() {
	err := rdb.XGroupCreateMkStream(context.Background(), cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Gagal membuat Consumer Group: %v", err)
	}
	log.Println("Consumer Group siap.")
}

// (Salin processMessage)
func processMessage(message redis.XMessage) {
	ctx := context.Background()
	apiKey := message.Values["source_app_key"].(string)
	payloadString := message.Values["payload"].(string)

	var payloadData map[string]interface{}
	if err := json.Unmarshal([]byte(payloadString), &payloadData); err != nil {
		log.Printf("ERROR [%s]: Gagal unmarshal JSON: %v", message.ID, err)
		rdb.XAck(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, message.ID)
		return
	}

	jobs, err := getFanOutJobs(apiKey)
	if err != nil {
		log.Printf("ERROR [%s]: Gagal mengambil integrasi: %v", message.ID, err)
		return
	}

	if len(jobs) == 0 {
		log.Printf("WARN [%s]: Tidak ada integrasi aktif ditemukan untuk key %s.", message.ID, apiKey)
		rdb.XAck(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, message.ID)
		return
	}

	log.Printf("INFO [%s]: Ditemukan %d integrasi (fan-out) untuk key %s", message.ID, len(jobs), apiKey)

	var wg sync.WaitGroup
	errChan := make(chan error, len(jobs))

	wg.Add(len(jobs))

	for _, job := range jobs {
		go func(j IntegrationJob) {
			defer wg.Done()
			if err := executeJob(message.ID, j, payloadData); err != nil {
				errChan <- err
			}
		}(job)
	}

	wg.Wait()
	close(errChan)

	var firstErr error
	for err := range errChan {
		if firstErr == nil {
			firstErr = err
		}
		log.Printf("ERROR in fan-out job [%s]: %v", message.ID, err)
	}

	if firstErr == nil {
		rdb.XAck(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, message.ID)
		log.Printf("COMPLETED [%s]: Selesai proses fan-out.", message.ID)
	} else {
		log.Printf("FAILED [%s]: Terjadi error, pesan TIDAK di-ACK dan akan dicoba lagi. Error pertama: %v", message.ID, firstErr)
	}
}

// (Salin executeJob)
func executeJob(msgID string, job IntegrationJob, payloadData map[string]interface{}) error {
	transformedData := transformData(payloadData, job.Mappings)
	if len(transformedData) == 0 {
		log.Printf("WARN [%s]: Tidak ada data yang di-map untuk subskripsi %d", msgID, job.SubscriptionID)
		return nil
	}

	targetDB, err := getTargetDB(job.Driver, job.TargetDSNFormat, job.EncryptedPass)
	if err != nil {
		errMessage := fmt.Errorf("gagal koneksi ke DB Tujuan (SubID %d): %v", job.SubscriptionID, err)
		log.Printf("ERROR [%s]: %v", msgID, errMessage)
		return errMessage
	}

	if err := insertData(targetDB, job.TargetTable, transformedData); err != nil {
		errMessage := fmt.Errorf("gagal INSERT ke '%s' (SubID %d): %v", job.TargetTable, job.SubscriptionID, err)
		log.Printf("ERROR [%s]: %v", msgID, errMessage)
		return errMessage
	}

	log.Printf("SUCCESS [%s]: Data berhasil dikirim ke '%s' (SubID %d)", msgID, job.TargetTable, job.SubscriptionID)
	return nil
}

// (Salin getFanOutJobs)
func getFanOutJobs(apiKey string) ([]IntegrationJob, error) {
	query := `
		SELECT
			ats.id AS subscription_id,
			dt.table_name,
			dc.connection_type AS driver,
			dc.host,
			dc.port,
			dc.database_name,
			dc.username,
			dc.password
		FROM
			applications AS app
		JOIN
			application_table_subscriptions AS ats ON app.id = ats.application_id
		JOIN
			database_tables AS dt ON ats.database_table_id = dt.id
		JOIN
			database_configs AS dc ON dt.database_config_id = dc.id
		WHERE
			app.app_key = ?
	`
	rows, err := nexusDB.Query(query, apiKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobsMap := make(map[int]*IntegrationJob)
	for rows.Next() {
		var subID sql.NullInt64
		var tableName, driver, host, port, dbName, user, pass sql.NullString

		err := rows.Scan(
			&subID, &tableName, &driver, &host, &port, &dbName, &user, &pass,
		)
		if err != nil {
			log.Printf("ERROR: Gagal scan row query: %v", err)
			continue
		}

		currentSubID := int(subID.Int64)
		if !subID.Valid {
			continue
		}

		job, exists := jobsMap[currentSubID]
		if !exists {
			job = &IntegrationJob{
				SubscriptionID: currentSubID,
				TargetTable:    tableName.String,
				Driver:         driver.String,
				EncryptedPass:  pass.String,
				Mappings:       make([]FieldMapping, 0),
			}

			if job.Driver == "mysql" {
				job.TargetDSNFormat = fmt.Sprintf("%s:%%s@tcp(%s:%s)/%s?parseTime=true",
					user.String, host.String, port.String, dbName.String)
			} else {
				log.Printf("WARN: Driver '%s' belum didukung", job.Driver)
				continue
			}
			jobsMap[currentSubID] = job
		}

		mappings, err := getMappings(currentSubID)
		if err != nil {
			log.Printf("ERROR: Gagal ambil mapping untuk SubID %d: %v", currentSubID, err)
		}
		job.Mappings = mappings
	}

	var jobs []IntegrationJob
	for _, job := range jobsMap {
		jobs = append(jobs, *job)
	}
	return jobs, nil
}

// (Salin getMappings)
func getMappings(subscriptionID int) ([]FieldMapping, error) {
	var mappings []FieldMapping
	rows, err := nexusDB.Query(`
		SELECT af.name, dfs.mapped_to
		FROM database_field_subscriptions dfs
		JOIN application_fields af ON dfs.application_field_id = af.id
		WHERE dfs.application_table_subscription_id = ?
	`, subscriptionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var m FieldMapping
		if err := rows.Scan(&m.SourceField, &m.TargetField); err != nil {
			return nil, err
		}
		mappings = append(mappings, m)
	}
	return mappings, nil
}

// (Salin getTargetDB)
func getTargetDB(driver string, dsnFormat string, encryptedPass string) (*sql.DB, error) {
	decryptMutex.RLock()
	plainPass, found := decryptionCache[encryptedPass]
	decryptMutex.RUnlock()

	if !found {
		log.Printf("Password terenkripsi tidak ditemukan di cache, memanggil API dekripsi...")
		var err error
		plainPass, err = callDecryptAPI(encryptedPass)
		if err != nil {
			return nil, fmt.Errorf("gagal mendekripsi password: %v", err)
		}

		decryptMutex.Lock()
		decryptionCache[encryptedPass] = plainPass
		decryptMutex.Unlock()
		log.Println("Password plain text berhasil didekripsi dan disimpan di cache.")
	}

	plainDSN := fmt.Sprintf(dsnFormat, plainPass)

	poolMutex.Lock()
	defer poolMutex.Unlock()

	if db, ok := targetDBPool[plainDSN]; ok {
		return db, nil
	}

	db, err := sql.Open(driver, plainDSN)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("gagal ping DB baru (plain DSN %s): %v", plainDSN, err)
	}

	targetDBPool[plainDSN] = db
	log.Printf("Membuat koneksi baru di pool.")
	return db, nil
}

// --- FUNGSI callDecryptAPI (PERUBAHAN KUNCI) ---
func callDecryptAPI(encryptedPassword string) (string, error) {

	// 1. Ambil token dari Environment, BUKAN dari cfg
	// Ini adalah cara paling sederhana dan pasti
	apiToken := os.Getenv("DECRYPTOR_API_TOKEN")
	fmt.Println(apiToken)
	if apiToken == "" {
		return "", fmt.Errorf("DECRYPTOR_API_TOKEN tidak diatur di environment")
	}

	// DEBUG: Cek apakah tokennya benar-benar ada
	// log.Printf("[DEBUG] Menggunakan token: %s", apiToken)

	// 2. Siapkan request ke API Laravel
	reqBody, _ := json.Marshal(map[string]string{
		"secret": encryptedPassword,
	})
	req, err := http.NewRequest("POST", cfg.Decryptor.ApiUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	// Gunakan token yang baru saja kita ambil dari os.Getenv()
	req.Header.Set("Authorization", "Bearer "+apiToken)

	// 3. Kirim request
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 4. Baca respons
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API dekripsi gagal dengan status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var respBody struct {
		Plain string `json:"plain"`
	}
	if err := json.Unmarshal(bodyBytes, &respBody); err != nil {
		return "", fmt.Errorf("gagal parse respons API dekripsi: %v", err)
	}

	if respBody.Plain == "" {
		return "", fmt.Errorf("API dekripsi mengembalikan password kosong")
	}

	return respBody.Plain, nil
}

// (Salin transformData)
func transformData(payload map[string]interface{}, mappings []FieldMapping) map[string]interface{} {
	transformed := make(map[string]interface{})
	for _, mapping := range mappings {
		if value, ok := payload[mapping.SourceField]; ok {
			transformed[mapping.TargetField] = value
		}
	}
	return transformed
}

// (Salin insertData)
func insertData(db *sql.DB, tableName string, data map[string]interface{}) error {
	var columns []string
	var placeholders []string
	var values []interface{}

	for col, val := range data {
		columns = append(columns, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	// columns = append(columns, "`created_at`", "`updated_at`")
	// placeholders = append(placeholders, "?", "?")
	// now := time.Now()
	// values = append(values, now, now)

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := db.Exec(query, values...)
	return err
}
