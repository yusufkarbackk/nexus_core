package main

import (
	"database/sql"
	"fmt"
	"strings"
)

func insertData(db *sql.DB, tableName string, data map[string]interface{}) error {
	var columns []string
	var placeholders []string
	var values []interface{}

	for col, val := range data {
		columns = append(columns, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	// Hapus blok created_at/updated_at yang di-hardcode
	/*
	   columns = append(columns, "`created_at`", "`updated_at`")
	   placeholders = append(placeholders, "?", "?")
	   now := time.Now()
	   values = append(values, now, now)
	*/

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := db.Exec(query, values...)
	return err
}
