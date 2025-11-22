package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

func getFanOutJobs(apiKey string) ([]IntegrationJob, error) {
	query := `
		SELECT
			ats.id AS subscription_id,
			dt.table_name,
			dc.connection_type AS driver,
			dc.host,
			dc.port,
			dc.database_name,
			dc.username,
			dc.password
		FROM
			applications AS app
		JOIN
			application_table_subscriptions AS ats ON app.id = ats.application_id
		JOIN
			database_tables AS dt ON ats.database_table_id = dt.id
		JOIN
			database_configs AS dc ON dt.database_config_id = dc.id
		WHERE
			app.app_key = ?
	`
	rows, err := nexusDB.Query(query, apiKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobsMap := make(map[int]*IntegrationJob)
	for rows.Next() {
		var subID sql.NullInt64
		var tableName, driver, host, port, dbName, user, pass sql.NullString

		err := rows.Scan(
			&subID, &tableName, &driver, &host, &port, &dbName, &user, &pass,
		)
		if err != nil {
			log.Printf("ERROR: Gagal scan row query: %v", err)
			continue
		}

		currentSubID := int(subID.Int64)
		if !subID.Valid {
			continue
		}

		job, exists := jobsMap[currentSubID]
		if !exists {
			job = &IntegrationJob{
				SubscriptionID: currentSubID,
				TargetTable:    tableName.String,
				Driver:         driver.String,
				EncryptedPass:  pass.String,
				Mappings:       make([]FieldMapping, 0),
			}

			if job.Driver == "mysql" {
				job.TargetDSNFormat = fmt.Sprintf("%s:%%s@tcp(%s:%s)/%s?parseTime=true",
					user.String, host.String, port.String, dbName.String)
			} else if job.Driver == "pgsql" || job.Driver == "postgres" || job.Driver == "postgresql" {
				// Postgres: postgres://user:%s@host:port/dbname?sslmode=disable
				// Perhatikan: Driver 'pq' mendukung format URL ini
				job.TargetDSNFormat = fmt.Sprintf("postgres://%s:%%s@%s:%s/%s?sslmode=disable",
					user.String, host.String, port.String, dbName.String)
			} else {
				log.Printf("WARN: Driver '%s' belum didukung", job.Driver)
				continue
			}
			jobsMap[currentSubID] = job
		}

		mappings, err := getMappings(currentSubID)
		if err != nil {
			log.Printf("ERROR: Gagal ambil mapping untuk SubID %d: %v", currentSubID, err)
		}
		job.Mappings = mappings
	}

	var jobs []IntegrationJob
	for _, job := range jobsMap {
		jobs = append(jobs, *job)
	}
	return jobs, nil
}
