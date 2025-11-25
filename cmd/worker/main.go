package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"nexus/nexusconfig"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// --- Variabel Global ---
var (
	rdb          *redis.Client
	nexusDB      *sql.DB
	cfg          *nexusconfig.Config
	targetDBPool = make(map[string]*sql.DB)
	poolMutex    = &sync.Mutex{}
	httpClient   = &http.Client{Timeout: 30 * time.Second}
)

const (
	DECRYPT_CACHE_TTL = 1 * time.Hour
	DEDUPLICATION_TTL = 24 * time.Hour
	RETRY_QUEUE_KEY   = "nexus:retry_queue"
	MAX_RETRIES       = 10
)

// --- Definisi Struct ---

type FieldMapping struct {
	SourceField sql.NullString
	TargetField sql.NullString
}

type IntegrationJob struct {
	SubscriptionID  int
	AppName         string
	TargetTable     string
	Driver          string
	TargetDSNFormat string
	EncryptedPass   string
	Mappings        []FieldMapping
}

type LogEntry struct {
	JobID        string
	DataID       string
	Source       string
	Destination  string
	Host         string
	DataSent     string
	DataReceived string
	Message      string
	Status       string
	SentAt       time.Time
}

type RetryPayload struct {
	MsgID           string                 `json:"msg_id"`
	SubscriptionID  int                    `json:"sub_id"`
	AppName         string                 `json:"app_name"`
	TargetTable     string                 `json:"target_table"`
	Driver          string                 `json:"driver"`
	TargetDSNFormat string                 `json:"dsn_fmt"`
	EncryptedPass   string                 `json:"enc_pass"`
	Data            map[string]interface{} `json:"data"`
	Attempt         int                    `json:"attempt"`
	LastError       string                 `json:"last_err"`
	OriginalPayload string                 `json:"orig_payload"`
	SentAt          time.Time              `json:"sent_at"`
}

// --- FUNGSI UTAMA ---

func main() {
	var err error
	if err := godotenv.Load(); err != nil {
		log.Fatal("KRITIS: Gagal memuat file .env. Pastikan .env ada di root.")
	}

	cfg, err = nexusconfig.LoadConfig()
	if err != nil {
		log.Fatalf("Gagal memuat config: %v", err)
	}

	rdb = redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
	})
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

	log.Println("Worker terhubung. Memulai sistem...")
	createConsumerGroup()

	// 1. Jalankan Retry Worker (Background)
	go startRetryWorker()

	// 2. Jalankan Main Worker (Redis Stream)
	startMainWorker()
}

func startMainWorker() {
	consumerName := "go-worker-" + getHostname()
	log.Println("Main Worker: Menunggu pesan di stream:", cfg.Redis.StreamName)

	for {
		// DENGAN ARSITEKTUR DELAYED RETRY, KITA TIDAK PERLU MENGAMBIL "0" (PENDING) DARI STREAM
		// KARENA RETRY WORKER MENGAMBILNYA DARI ZSET.
		// Main Worker HANYA AMBIL PESAN BARU (>) DAN TIDUR EFISIEN (BLOCK).
		streams, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    cfg.Redis.ConsumerGroup,
			Consumer: consumerName,
			Streams:  []string{cfg.Redis.StreamName, ">"}, // HANYA PESAN BARU
			Count:    10,
			Block:    2 * time.Second, // TIDUR EFISIEN
		}).Result()

		if err != nil && err != redis.Nil {
			log.Printf("Main Worker ERROR: %v. Beri jeda 5s.", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(streams) > 0 {
			for _, stream := range streams {
				for _, message := range stream.Messages {
					log.Printf("Menerima pesan ID: %s (stream: '>')", message.ID)
					go processMessage(message)
				}
			}
		}
	}
}

// --- RETRY WORKER ---
func startRetryWorker() {
	log.Println("Retry Worker: Siap memproses kegagalan...")
	for {
		processRetryQueue()
		time.Sleep(1 * time.Second) // Cek setiap 1 detik
	}
}

func processRetryQueue() {
	ctx := context.Background()
	now := float64(time.Now().Unix())

	results, err := rdb.ZRangeByScore(ctx, RETRY_QUEUE_KEY, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", now),
		Count: 10,
	}).Result()

	if err != nil || len(results) == 0 {
		return
	}

	log.Printf("Retry Worker: Memproses %d tugas gagal...", len(results))

	for _, itemStr := range results {
		removed, _ := rdb.ZRem(ctx, RETRY_QUEUE_KEY, itemStr).Result()
		if removed == 0 {
			continue
		}

		var task RetryPayload
		if err := json.Unmarshal([]byte(itemStr), &task); err != nil {
			log.Printf("Retry Worker: Gagal unmarshal: %v. Task dibuang.", err)
			continue
		}

		log.Printf("RETRY [%s]: Mencoba lagi ke '%s' (Attempt %d)", task.MsgID, task.TargetTable, task.Attempt)

		logEntry := LogEntry{
			JobID: task.MsgID, Source: task.AppName, Destination: task.TargetTable, Host: getHostname(),
			DataSent: task.OriginalPayload, SentAt: task.SentAt, Status: "RETRY_FAILED",
			Message: fmt.Sprintf("Attempt %d failed: %v", task.Attempt, task.LastError),
		}

		// Perform insert
		err = performDatabaseInsert(task.MsgID, task.SubscriptionID, task.Driver, task.TargetDSNFormat, task.EncryptedPass, task.TargetTable, task.Data)

		if err != nil {
			log.Printf("RETRY FAILED [%s]: %v", task.MsgID, err)

			task.LastError = err.Error()
			scheduleRetry(task)
		} else {
			log.Printf("RETRY SUCCESS [%s]: Berhasil pulih!", task.MsgID)

			logEntry.Status = "SUCCESS"
			logEntry.Message = fmt.Sprintf("Recovered on attempt %d", task.Attempt)
			logToNexus(logEntry)
		}
	}
}

func scheduleRetry(task RetryPayload) {
	task.Attempt++
	if task.Attempt > MAX_RETRIES {
		log.Printf("DROP [%s]: Max retries (%d) reached. Task dibuang.", task.MsgID, MAX_RETRIES)
		return
	}

	delay := time.Duration(5*(1<<(task.Attempt-1))) * time.Second
	nextTry := float64(time.Now().Add(delay).Unix())

	taskJSON, _ := json.Marshal(task)

	err := rdb.ZAdd(context.Background(), RETRY_QUEUE_KEY, &redis.Z{
		Score:  nextTry,
		Member: string(taskJSON),
	}).Err()

	if err != nil {
		log.Printf("KRITIS: Gagal menyimpan retry ke Redis: %v", err)
	} else {
		log.Printf("SCHEDULE [%s]: Dijadwalkan ulang dalam %v ke '%s'", task.MsgID, delay, task.TargetTable)
	}
}

// --- STREAM PROCESSING & EXECUTION ---

func processStreamMessages(consumerName string, streamID string) (int, error) {
	ctx := context.Background()
	var blockDuration time.Duration
	if streamID == ">" {
		blockDuration = 2 * time.Second
	}

	streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    cfg.Redis.ConsumerGroup,
		Consumer: consumerName,
		Streams:  []string{cfg.Redis.StreamName, streamID},
		Count:    10,
		Block:    blockDuration,
	}).Result()

	if err != nil {
		return 0, err
	}

	processedCount := 0
	for _, stream := range streams {
		for _, message := range stream.Messages {
			log.Printf("Menerima pesan ID: %s (stream: '%s')", message.ID, streamID)
			go processMessage(message)
			processedCount++
		}
	}
	return processedCount, nil
}

func createConsumerGroup() {
	err := rdb.XGroupCreateMkStream(context.Background(), cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Gagal membuat Consumer Group: %v", err)
	}
}

func processMessage(message redis.XMessage) {
	ctx := context.Background()
	apiKey := message.Values["source_app_key"].(string)
	payloadString := message.Values["payload"].(string)

	sentAt := time.Now()
	if val, ok := message.Values["received_at"].(string); ok {
		if parsedTime, err := time.Parse(time.RFC3339, val); err == nil {
			sentAt = parsedTime
		}
	}

	var payloadData map[string]interface{}
	if err := json.Unmarshal([]byte(payloadString), &payloadData); err != nil {
		log.Printf("ERROR [%s]: Json invalid. ACK & Drop.", message.ID)
		rdb.XAck(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, message.ID)
		return
	}

	jobs, err := getFanOutJobs(apiKey)
	if err != nil {
		log.Printf("ERROR [%s]: Gagal ambil config. Skip ACK.", message.ID)
		return
	}

	if len(jobs) == 0 {
		log.Printf("WARN [%s]: Tidak ada integrasi. ACK.", message.ID)
		rdb.XAck(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, message.ID)
		return
	}

	appName := jobs[0].AppName
	log.Printf("INFO [%s]: Fan-out: %s (%d tujuan)", message.ID, appName, len(jobs))

	var wg sync.WaitGroup
	wg.Add(len(jobs))

	dataID := message.ID
	if id, ok := payloadData["id"].(string); ok {
		dataID = id
	}
	if id, ok := payloadData["user_id"].(string); ok {
		dataID = id
	}
	if id, ok := payloadData["order_id"].(string); ok {
		dataID = id
	}

	for _, job := range jobs {
		logEntry := LogEntry{
			DataID: dataID, Source: job.AppName, Host: getHostname(), DataSent: payloadString, SentAt: sentAt, Destination: job.TargetTable, JobID: message.ID,
		}

		go func(j IntegrationJob, l LogEntry) {
			defer wg.Done()
			executeMainJob(j, payloadData, l)
		}(job, logEntry)
	}

	wg.Wait()
	// KUNCI PERBAIKAN: SELALU ACK DI SINI! (Karena yang gagal sudah dialihkan ke ZSET)
	rdb.XAck(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, message.ID)
	log.Printf("COMPLETED [%s]: Stream selesai.", message.ID)
}

// executeMainJob: Mengirim tugas ke performDatabaseInsert atau Schedule Retry
func executeMainJob(job IntegrationJob, payloadData map[string]interface{}, logEntry LogEntry) {
	// 1. Transformasi
	transformedData := transformData(payloadData, job.Mappings)
	transJson, _ := json.Marshal(transformedData)
	logEntry.DataReceived = string(transJson)

	if len(transformedData) == 0 {
		logEntry.Status = "WARN"
		logEntry.Message = "Tidak ada data mapping valid"
		logToNexus(logEntry)
		return
	}

	// 2. Coba Insert Langsung
	err := performDatabaseInsert(logEntry.JobID, job.SubscriptionID, job.Driver, job.TargetDSNFormat, job.EncryptedPass, job.TargetTable, transformedData)

	if err != nil {
		// 2a. Cek duplikasi (Idempotency)
		if isDuplicateError(err) {
			logEntry.Status = "SUCCESS"
			logEntry.Message = "Duplicate skipped (Idempotent)"
			logToNexus(logEntry)
			return
		}

		// 2b. Jika error koneksi/lainnya -> Schedule Retry
		retryTask := RetryPayload{
			MsgID: logEntry.JobID, SubscriptionID: job.SubscriptionID, AppName: job.AppName, TargetTable: job.TargetTable,
			Driver: job.Driver, TargetDSNFormat: job.TargetDSNFormat, EncryptedPass: job.EncryptedPass,
			Data: transformedData, Attempt: 1, LastError: err.Error(), OriginalPayload: logEntry.DataSent, SentAt: logEntry.SentAt,
		}
		scheduleRetry(retryTask)

		// Log sebagai RETRY
		logEntry.Status = "RETRY"
		logEntry.Message = fmt.Sprintf("Queued for retry: %v", err)
		logToNexus(logEntry)
		return
	}

	// Sukses
	logEntry.Status = "SUCCESS"
	logEntry.Message = "Data inserted successfully"
	logToNexus(logEntry)
}

func performDatabaseInsert(msgID string, subID int, driver, dsnFmt, encPass, table string, data map[string]interface{}) error {
	// 1. Cek Deduplikasi Redis
	dedupKey := fmt.Sprintf("nexus:dedup:%s:%d", msgID, subID)
	exists, err := rdb.Exists(context.Background(), dedupKey).Result()
	if err == nil && exists > 0 {
		log.Printf("INFO [%s]: Job duplikat (Redis). Skip.", msgID)
		return nil
	}

	// 2. Koneksi DB
	targetDB, err := getTargetDB(driver, dsnFmt, encPass)
	if err != nil {
		return fmt.Errorf("koneksi DB gagal: %v", err)
	}

	// 3. Insert
	if err := insertData(targetDB, driver, table, data); err != nil {
		return err
	}

	// 4. Simpan Deduplikasi
	rdb.Set(context.Background(), dedupKey, "1", DEDUPLICATION_TTL)
	return nil
}

// --- FUNGSI HELPER & DB (Implementasi Penuh) ---

func getTargetDB(driver, dsnFmt, encryptedPass string) (*sql.DB, error) {
	hasher := sha1.New()
	hasher.Write([]byte(encryptedPass))
	cacheKey := "nexus_decrypt_cache:" + hex.EncodeToString(hasher.Sum(nil))

	ctx := context.Background()
	plainPass, err := rdb.Get(ctx, cacheKey).Result()

	if err == redis.Nil {
		plainPass, err = callDecryptAPI(encryptedPass)
		if err != nil {
			return nil, err
		}
		rdb.Set(ctx, cacheKey, plainPass, DECRYPT_CACHE_TTL)
	} else if err != nil {
		return nil, err
	}

	plainDSN := fmt.Sprintf(dsnFmt, plainPass)
	poolMutex.Lock()
	defer poolMutex.Unlock()

	if db, ok := targetDBPool[plainDSN]; ok {
		return db, nil
	}

	driverName := driver
	if driver == "pgsql" || driver == "postgresql" {
		driverName = "postgres"
	}

	db, err := sql.Open(driverName, plainDSN)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("gagal ping DB: %v", err)
	}
	targetDBPool[plainDSN] = db
	return db, nil
}

func callDecryptAPI(encryptedPassword string) (string, error) {
	apiToken := os.Getenv("DECRYPTOR_API_TOKEN")
	if apiToken == "" {
		return "", fmt.Errorf("token API tidak ditemukan")
	}

	reqBody, _ := json.Marshal(map[string]string{"secret": encryptedPassword})
	req, _ := http.NewRequest("POST", cfg.Decryptor.ApiUrl, bytes.NewBuffer(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

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

func transformData(payload map[string]interface{}, mappings []FieldMapping) map[string]interface{} {
	transformed := make(map[string]interface{})
	for _, mapping := range mappings {
		if mapping.SourceField.String == "" {
			continue
		}
		if val, ok := payload[mapping.SourceField.String]; ok {
			transformed[mapping.TargetField.String] = val
		}
	}
	return transformed
}

func insertData(db *sql.DB, driver, tableName string, data map[string]interface{}) error {
	quote := func(s string) string {
		if driver == "pgsql" || driver == "postgres" || driver == "postgresql" {
			return `"` + s + `"`
		}
		return "`" + s + "`"
	}
	var columns, placeholders []string
	var values []interface{}
	i := 1
	for col, val := range data {
		columns = append(columns, quote(col))
		if driver == "pgsql" || driver == "postgres" || driver == "postgresql" {
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			i++
		} else {
			placeholders = append(placeholders, "?")
		}
		values = append(values, val)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quote(tableName), strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	_, err := db.Exec(query, values...)
	return err
}

func isDuplicateError(err error) bool {
	if strings.Contains(err.Error(), "Error 1062") {
		return true
	}
	if strings.Contains(err.Error(), "23505") {
		return true
	}
	return false
}

func logToNexus(entry LogEntry) {
	query := `INSERT INTO logs (data_id, source, destination, host, data_sent, data_received, sent_at, received_at, message, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?, NOW(), NOW())`
	nexusDB.Exec(query, entry.DataID, entry.Source, entry.Destination, entry.Host, entry.DataSent, entry.DataReceived, entry.SentAt, entry.Message, entry.Status)
}

func getFanOutJobs(apiKey string) ([]IntegrationJob, error) {
	query := `SELECT ats.id AS subscription_id, app.name AS app_name, dt.table_name, dc.connection_type AS driver, dc.host, dc.port, dc.database_name, dc.username, dc.password, af.name AS source_field, dfs.mapped_to AS target_field FROM applications AS app JOIN application_table_subscriptions AS ats ON app.id = ats.application_id JOIN database_tables AS dt ON ats.database_table_id = dt.id JOIN database_configs AS dc ON dt.database_config_id = dc.id LEFT JOIN database_field_subscriptions AS dfs ON ats.id = dfs.application_table_subscription_id LEFT JOIN application_fields AS af ON dfs.application_field_id = af.id WHERE app.app_key = ?`
	rows, err := nexusDB.Query(query, apiKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobsMap := make(map[int]*IntegrationJob)
	for rows.Next() {
		var subID sql.NullInt64
		var appName, tableName, driver, host, port, dbName, user, pass sql.NullString
		var fm FieldMapping
		err := rows.Scan(&subID, &appName, &tableName, &driver, &host, &port, &dbName, &user, &pass, &fm.SourceField, &fm.TargetField)
		if err != nil || !subID.Valid {
			continue
		}

		currentSubID := int(subID.Int64)
		job, exists := jobsMap[currentSubID]
		if !exists {
			job = &IntegrationJob{SubscriptionID: currentSubID, AppName: appName.String, TargetTable: tableName.String, Driver: driver.String, EncryptedPass: pass.String, Mappings: []FieldMapping{}}
			if job.Driver == "mysql" {
				job.TargetDSNFormat = fmt.Sprintf("%s:%%s@tcp(%s:%s)/%s?parseTime=true", user.String, host.String, port.String, dbName.String)
			} else {
				job.TargetDSNFormat = fmt.Sprintf("postgres://%s:%%s@%s:%s/%s?sslmode=disable", user.String, host.String, port.String, dbName.String)
			}
			jobsMap[currentSubID] = job
		}
		if fm.SourceField.Valid && fm.TargetField.Valid {
			job.Mappings = append(job.Mappings, fm)
		}
	}
	var jobs []IntegrationJob
	for _, job := range jobsMap {
		jobs = append(jobs, *job)
	}
	return jobs, nil
}

func getHostname() string { h, _ := os.Hostname(); return h }
