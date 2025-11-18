package main

func transformData(payload map[string]interface{}, mappings []FieldMapping) map[string]interface{} {
	transformed := make(map[string]interface{})
	for _, mapping := range mappings {
		if value, ok := payload[mapping.SourceField]; ok {
			transformed[mapping.TargetField] = value
		}
	}
	return transformed
}
