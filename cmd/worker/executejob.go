package main

import (
	"fmt"
	"log"
)

// (executeJob dan fungsi lainnya tetap sama persis)
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
