package main

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
)

// (Fungsi processMessage() tetap sama, logikanya sudah benar)
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
		return // JANGAN ACK
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
		// JANGAN ACK, loop 'main' akan mengambilnya lagi via ID "0"
	}
}
