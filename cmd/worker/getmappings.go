package main

func getMappings(subscriptionID int) ([]FieldMapping, error) {
	var mappings []FieldMapping
	rows, err := nexusDB.Query(`
		SELECT af.name, dfs.mapped_to
		FROM database_field_subscriptions dfs
		JOIN application_fields af ON dfs.application_field_id = af.id
		WHERE dfs.application_table_subscription_id = ?
	`, subscriptionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var m FieldMapping
		if err := rows.Scan(&m.SourceField, &m.TargetField); err != nil {
			return nil, err
		}
		mappings = append(mappings, m)
	}
	return mappings, nil
}
