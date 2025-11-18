package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
)

func getDecryptedPassword(encryptedPass string) (string, error) {
	ctx := context.Background()

	// 1. Buat kunci cache yang aman
	// Kita hash password terenkripsi agar tidak menyimpannya sebagai kunci
	hasher := sha1.New()
	hasher.Write([]byte(encryptedPass))
	cacheKey := "nexus_decrypt_cache:" + hex.EncodeToString(hasher.Sum(nil))

	// 2. Coba ambil dari Cache Redis
	plainPass, err := rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		// Cache HIT (Ditemukan)
		log.Println("Password plain text diambil dari cache Redis.")
		return plainPass, nil
	}

	// 3. Cache MISS (Tidak Ditemukan), panggil API Dekripsi
	if err != redis.Nil {
		// Ini adalah error Redis yang sebenarnya, bukan hanya cache miss
		log.Printf("WARN: Gagal mengambil cache Redis: %v. Melanjutkan dengan API.", err)
	}

	log.Printf("Password terenkripsi tidak ditemukan di cache Redis, memanggil API dekripsi...")
	plainPass, err = callDecryptAPI(encryptedPass)
	if err != nil {
		return "", fmt.Errorf("gagal mendekripsi password: %v", err)
	}

	// 4. Simpan hasil ke Cache Redis dengan TTL (Waktu Kadaluarsa)
	err = rdb.Set(ctx, cacheKey, plainPass, DECRYPT_CACHE_TTL).Err()
	if err != nil {
		// Gagal menyimpan cache, tapi tidak apa-apa, proses lanjut
		log.Printf("WARN: Gagal menyimpan cache dekripsi ke Redis: %v", err)
	}

	log.Println("Password plain text berhasil didekripsi dan disimpan di cache Redis.")
	return plainPass, nil
}
