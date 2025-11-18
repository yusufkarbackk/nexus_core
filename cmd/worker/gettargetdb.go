package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

func getTargetDB(driver string, dsnFormat string, encryptedPass string) (*sql.DB, error) {

	// 1. Dapatkan password plain text (dari cache Redis atau API)
	plainPass, err := getDecryptedPassword(encryptedPass)
	if err != nil {
		return nil, err
	}

	// 2. Bangun DSN plain text
	plainDSN := fmt.Sprintf(dsnFormat, plainPass)

	// 3. Gunakan DSN plain text untuk mengelola pool koneksi (Logika ini tetap sama)
	poolMutex.Lock()
	defer poolMutex.Unlock()

	if db, ok := targetDBPool[plainDSN]; ok {
		return db, nil // Kembalikan koneksi yang ada
	}

	// 4. Buat koneksi baru
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
