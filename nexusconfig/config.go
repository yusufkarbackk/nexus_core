package nexusconfig

import (
	"log"
	"strings"

	// "github.com/joho/godotenv" // <-- HAPUS IMPORT INI
	"github.com/spf13/viper"
)

// (Struct Config tetap sama persis)
type Config struct {
	Server struct {
		Port string
	}
	Redis struct {
		Addr          string
		StreamName    string
		ConsumerGroup string
	}
	Database struct {
		NexusDBDSN string `mapstructure:"nexus_db_dsn"`
	}
	Decryptor struct {
		ApiUrl   string `mapstructure:"api_url"`
	}
}

var VP *viper.Viper

func LoadConfig() (*Config, error) {

	// HAPUS FUNGSI godotenv.Load() DARI SINI

	VP = viper.New()

	// 1. Beritahu Viper untuk juga membaca dari Environment Variables
	// Ini PENTING. Ini akan mencari DECRYPTOR_APITOKEN
	VP.AutomaticEnv()
	VP.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// 2. Atur path untuk file konfigurasi
	VP.AddConfigPath(".") // Cari di folder root

	// 3. Baca config.yml (File non-rahasia)
	VP.SetConfigName("config")
	VP.SetConfigType("yaml")
	if err := VP.ReadInConfig(); err != nil {
		log.Printf("Peringatan: Gagal membaca file config.yml: %v", err)
	}

	// 4. BACA .env (File rahasia)
	// Kita beritahu Viper untuk membaca file .env JUGA
	VP.SetConfigName(".env")
	VP.SetConfigType("env")
	// GABUNGKAN (Merge) isi .env di atas apa yang ada di config.yml
	if err := VP.MergeInConfig(); err != nil {
		log.Printf("Peringatan: Gagal membaca file .env: %v", err)
	}

	// 5. Unmarshal gabungan hasilnya ke struct
	var config Config
	if err := VP.Unmarshal(&config); err != nil {
		log.Fatalf("Gagal unmarshal konfigurasi: %s", err)
	}

	// Cek log debug lagi
	// log.Printf("[DEBUG] Token yang dimuat dari Viper: %s", config.Decryptor.ApiToken)

	log.Println("Konfigurasi Golang berhasil dimuat.")
	return &config, nil
}
