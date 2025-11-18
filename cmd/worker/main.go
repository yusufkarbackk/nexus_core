package main

import (
	
	"context"
	"database/sql"
	
	
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	
	"log"
	"net/http"
	"nexus/nexusconfig"
	
	"strings"
	"sync"
	"time"
)

// (Semua 'var' global tetap sama)
var (
	rdb          *redis.Client
	nexusDB      *sql.DB
	cfg          *nexusconfig.Config
	targetDBPool = make(map[string]*sql.DB)
	poolMutex    = &sync.Mutex{}
	// decryptionCache = make(map[string]string)
	// decryptMutex    = &sync.RWMutex{}
	httpClient = &http.Client{Timeout: 30 * time.Second}
)

const DECRYPT_CACHE_TTL = 1 * time.Hour // Simpan password plain text selama 1 jam

// (Semua 'struct' tetap sama)
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
	if err := godotenv.Load(); err != nil {
		log.Fatal("KRITIS: Gagal memuat file .env. Pastikan .env ada di root.")
	}
	log.Println("File .env berhasil dimuat.")

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

	log.Println("Worker terhubung ke Redis dan Nexus DB.")
	createConsumerGroup()
	log.Println("Menunggu pesan di stream:", cfg.Redis.StreamName)

	// --- LOOP UTAMA (DIUBAH) ---
	// 'consumerName' unik per worker, misal dari hostname
	consumerName := "go-worker-" + getHostname()

	for {
		// 1. SELALU proses pesan yang tertunda (gagal) terlebih dahulu
		// Kita menggunakan ID "0" untuk membaca Pending Entries List (PEL)
		processedPending, err := processStreamMessages(consumerName, "0")
		if err != nil {
			log.Printf("ERROR saat memproses pesan tertunda: %v", err)
			time.Sleep(5 * time.Second) // Beri jeda jika ada error
		}

		// 2. Jika tidak ada pesan tertunda, baru ambil pesan baru
		// Kita menggunakan ID ">" untuk pesan baru
		if processedPending == 0 {
			_, err := processStreamMessages(consumerName, ">")
			if err != nil {
				// Jika errornya bukan timeout, log
				if err != redis.Nil {
					log.Printf("ERROR saat memproses pesan baru: %v", err)
				}
				// Jika redis.Nil (timeout), itu normal, loop akan berulang
			}
		}
		// Loop berlanjut, selalu cek pesan '0' dulu
	}
}

// (Fungsi createConsumerGroup() tetap sama)
func createConsumerGroup() {
	err := rdb.XGroupCreateMkStream(context.Background(), cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("Gagal membuat Consumer Group: %v", err)
	}
	log.Println("Consumer Group siap.")
}

