package main

import (
	"os"
)

// Fungsi helper untuk mendapatkan hostname
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		// Fallback jika gagal
		return "unknown-worker"
	}
	return hostname
}
