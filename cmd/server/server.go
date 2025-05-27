package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/sifes/architecture-practice-5/httptools"
	"github.com/sifes/architecture-practice-5/signal"
)

var port = flag.Int("port", 8080, "server port")
var dbAddress = flag.String("db-address", "http://db:8070", "database service address")
var teamName = flag.String("team", "telepuziki", "team name for initialization")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

type keyValueResponse struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type valueRequest struct {
	Value interface{} `json:"value"`
}

func main() {
	flag.Parse()

	// Initialize team data in database
	if err := initializeTeamData(); err != nil {
		log.Printf("Warning: Failed to initialize team data: %v", err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		// Get key parameter from URL
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "Missing key parameter", http.StatusBadRequest)
			return
		}

		// Query database service
		dbURL := fmt.Sprintf("%s/db/%s", *dbAddress, key)
		resp, err := http.Get(dbURL)
		if err != nil {
			log.Printf("Failed to query database: %v", err)
			http.Error(rw, "Database connection error", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Database returned status %d", resp.StatusCode)
			http.Error(rw, "Database error", http.StatusInternalServerError)
			return
		}

		// Read and parse response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read database response: %v", err)
			http.Error(rw, "Failed to read database response", http.StatusInternalServerError)
			return
		}

		var dbResponse keyValueResponse
		if err := json.Unmarshal(body, &dbResponse); err != nil {
			log.Printf("Failed to parse database response: %v", err)
			http.Error(rw, "Invalid database response", http.StatusInternalServerError)
			return
		}

		// Return the value as JSON array for backward compatibility
		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			fmt.Sprintf("%v", dbResponse.Value),
		})
	})

	// Additional endpoints remain unchanged
	h.HandleFunc("/api/v1/some-data-check1", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"2", "3",
		})
	})

	h.HandleFunc("/api/v1/some-data-check2", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"3", "4",
		})
	})

	h.HandleFunc("/api/v1/some-data-check3", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"4", "5",
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func initializeTeamData() error {
	// Store current date under team name key
	currentDate := time.Now().Format("2006-01-02")
	
	reqBody := valueRequest{
		Value: currentDate,
	}
	
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	dbURL := fmt.Sprintf("%s/db/%s", *dbAddress, *teamName)
	
	// Retry a few times in case database is still starting
	for i := 0; i < 5; i++ {
		resp, err := http.Post(dbURL, "application/json", bytes.NewReader(jsonData))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Printf("Successfully initialized team data: %s = %s", *teamName, currentDate)
				return nil
			}
			log.Printf("Database returned status %d when initializing team data", resp.StatusCode)
		} else {
			log.Printf("Failed to connect to database (attempt %d/5): %v", i+1, err)
		}
		
		if i < 4 {
			time.Sleep(2 * time.Second)
		}
	}

	return fmt.Errorf("failed to initialize team data after 5 attempts")
}