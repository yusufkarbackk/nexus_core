package main

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

// FUNGSI BARU: processStreamMessages (menggabungkan logika loop)
// ID bisa "0" (pending) atau ">" (baru)
func processStreamMessages(consumerName string, streamID string) (int, error) {
	ctx := context.Background()

	// Set 'Block' ke 0 jika kita cek pending, agar tidak menunggu
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
		return 0, err // Bisa jadi redis.Nil (timeout), itu normal
	}

	processedCount := 0
	for _, stream := range streams {
		for _, message := range stream.Messages {
			log.Printf("Menerima pesan ID: %s (dari ID: '%s')", message.ID, streamID)
			processMessage(message) // proses seperti biasa
			processedCount++
		}
	}
	return processedCount, nil
}
