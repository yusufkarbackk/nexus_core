package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func callDecryptAPI(encryptedPassword string) (string, error) {
	apiToken := os.Getenv("DECRYPTOR_API_TOKEN")
	if apiToken == "" {
		return "", fmt.Errorf("DECRYPTOR_API_TOKEN tidak diatur di environment")
	}

	reqBody, _ := json.Marshal(map[string]string{
		"secret": encryptedPassword,
	})
	req, err := http.NewRequest("POST", cfg.Decryptor.ApiUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API dekripsi gagal dengan status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var respBody struct {
		Plain string `json:"plain"`
	}
	if err := json.Unmarshal(bodyBytes, &respBody); err != nil {
		return "", fmt.Errorf("gagal parse respons API dekripsi: %v", err)
	}

	if respBody.Plain == "" {
		return "", fmt.Errorf("API dekripsi mengembalikan password kosong")
	}

	return respBody.Plain, nil
}
