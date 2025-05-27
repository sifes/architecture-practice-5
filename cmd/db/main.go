package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/sifes/architecture-practice-5/datastore"
	"github.com/sifes/architecture-practice-5/httptools"
	"github.com/sifes/architecture-practice-5/signal"
)

var port = flag.Int("port", 8070, "database server port")
var dir = flag.String("dir", "/opt/practice-4/data", "database directory")

type keyValueResponse struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type valueRequest struct {
	Value interface{} `json:"value"`
}

func main() {
	flag.Parse()

	// Open database
	db, err := datastore.Open(*dir)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	h := new(http.ServeMux)

	// GET /db/<key>
	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/db/")
		
		if r.Method == http.MethodGet {
			handleGet(db, key, rw, r)
		} else if r.Method == http.MethodPost {
			handlePost(db, key, rw, r)
		} else {
			http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("Starting database server on port %d...", *port)
	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func handleGet(db *datastore.Db, key string, rw http.ResponseWriter, r *http.Request) {
	if key == "" {
		http.Error(rw, "Key is required", http.StatusBadRequest)
		return
	}

	// Check for type parameter (for variant 4)
	valueType := r.URL.Query().Get("type")
	if valueType == "" {
		valueType = "string"
	}

	var value interface{}
	var err error

	switch valueType {
	case "int64":
		value, err = db.GetInt64(key)
	case "string":
		value, err = db.Get(key)
	default:
		http.Error(rw, "Invalid type parameter", http.StatusBadRequest)
		return
	}

	if err != nil {
		if err == datastore.ErrNotFound {
			http.Error(rw, "Not found", http.StatusNotFound)
			return
		}
		// For variant 3: hash sum errors would also return 404
		http.Error(rw, "Not found", http.StatusNotFound)
		return
	}

	response := keyValueResponse{
		Key:   key,
		Value: value,
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(response)
}

func handlePost(db *datastore.Db, key string, rw http.ResponseWriter, r *http.Request) {
	if key == "" {
		http.Error(rw, "Key is required", http.StatusBadRequest)
		return
	}

	var req valueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(rw, "Invalid request body", http.StatusBadRequest)
		return
	}

	var err error
	
	// Determine value type and call appropriate Put method
	switch v := req.Value.(type) {
	case string:
		err = db.Put(key, v)
	case float64:
		// JSON numbers are decoded as float64, convert to int64
		err = db.PutInt64(key, int64(v))
	default:
		// Try to convert to string
		err = db.Put(key, fmt.Sprintf("%v", v))
	}

	if err != nil {
		http.Error(rw, "Failed to store value", http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	fmt.Fprint(rw, "OK")
}