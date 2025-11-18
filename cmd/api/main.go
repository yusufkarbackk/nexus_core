package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	// Import paket config kita dari folder nexusconfig/
	// 'nexus' adalah nama modul dari go.mod
	"nexus/nexusconfig"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql" // Driver MySQL
)

var (
	rdb         *redis.Client
	db          *sql.DB                 // Koneksi ke nexus_db (untuk validasi)
	cfg         *nexusconfig.Config     // Objek konfigurasi
	apiKeyCache = make(map[string]bool) // Cache sederhana untuk API Key
	cacheMutex  = &sync.RWMutex{}       // Mutex untuk melindungi cache
)

func main() {
	var err error
	// 1. Muat Konfigurasi dari config.yml
	cfg, err = nexusconfig.LoadConfig()
	if err != nil {
		log.Fatalf("Gagal memuat config: %v", err)
	}

	// 2. Koneksi ke Redis
	rdb = redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
	})
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Gagal koneksi ke Redis: %v", err)
	}
	log.Println("API terhubung ke Redis.")

	// 3. Koneksi ke Nexus DB (untuk validasi API Key)
	db, err = sql.Open("mysql", cfg.Database.NexusDBDSN)
	if err != nil {
		log.Fatalf("Gagal koneksi ke Nexus DB: %v", err)
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	log.Println("API terhubung ke Nexus DB.")

	// 4. Setup Gin Router
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/ingress", handleIngress) // Endpoint penerima data

	log.Printf("Menjalankan API Ingress Golang di port %s...", cfg.Server.Port)
	// Jalankan server HTTP
	router.Run(":" + cfg.Server.Port)
}

// handleIngress adalah pengganti DataController.store
func handleIngress(c *gin.Context) {
	apiKey := c.GetHeader("X-API-Key")

	// 1. Validasi API Key (dengan cache)
	if !isValidAPIKey(apiKey) {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid API Key"})
		return
	}

	// 2. Bind JSON Payload
	var jsonData map[string]interface{}
	if err := c.BindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid JSON payload"})
		return
	}

	// 3. Serialize payload untuk disimpan di Redis
	payloadBytes, err := json.Marshal(jsonData)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to serialize payload"})
		return
	}

	// 4. Siapkan data untuk Redis Stream
	streamData := map[string]interface{}{
		"source_app_key": apiKey,
		"payload":        string(payloadBytes), // Simpan sebagai string JSON
		"received_at":    time.Now().Format(time.RFC3339),
	}

	// 5. Kirim ke Redis Stream (XADD)
	msgID, err := rdb.XAdd(c, &redis.XAddArgs{
		Stream: cfg.Redis.StreamName,
		Values: streamData,
	}).Result()

	if err != nil {
		log.Printf("ERROR: Gagal XADD ke Stream: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Failed to queue data"})
		return
	}

	// 6. Berhasil
	c.JSON(http.StatusAccepted, gin.H{
		"message":    "Data accepted",
		"message_id": msgID,
	})
}

// isValidAPIKey memeriksa apakah API key valid, menggunakan cache
func isValidAPIKey(apiKey string) bool {
	cacheMutex.RLock()
	isValid, found := apiKeyCache[apiKey]
	cacheMutex.RUnlock()

	if found {
		return isValid // Kembalikan dari cache
	}

	// Jika tidak ada di cache, query ke DB
	var exists bool
	// Query ke DB yang diisi oleh Filament
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM applications WHERE app_key = ?)", apiKey).Scan(&exists)
	//log.Printf(err.Error())
	if err != nil {
		log.Printf("ERROR: Gagal query validasi app Key: %v", err)
		return false // Anggap tidak valid jika DB error
	}

	// Simpan di cache
	cacheMutex.Lock()
	apiKeyCache[apiKey] = exists
	cacheMutex.Unlock()

	return exists
}
