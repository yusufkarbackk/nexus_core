package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
)

func insertData(db *sql.DB, driver string, tableName string, data map[string]interface{}) error {
	quote := func(s string) string {
		if driver == "pgsql" || driver == "postgres" || driver == "postgresql" {
			return `"` + s + `"` // Postgres
		}
		return "`" + s + "`" // MySQL
	}

	var columns []string
	var placeholders []string
	var values []interface{}

	i := 1
	for col, val := range data {
		columns = append(columns, quote(col))

		if driver == "pgsql" || driver == "postgres" || driver == "postgresql" {
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			i++
		} else {
			placeholders = append(placeholders, "?")
		}
		values = append(values, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quote(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := db.Exec(query, values...)
	return err
}
