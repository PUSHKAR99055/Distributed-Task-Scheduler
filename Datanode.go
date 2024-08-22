package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
)

const dataDir = "./data"

// handleWrite stores the data block in a file.
func handleWrite(w http.ResponseWriter, r *http.Request) {
	blockID := r.URL.Query().Get("blockID")
	if blockID == "" {
		http.Error(w, "blockID missing", http.StatusBadRequest)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read data", http.StatusInternalServerError)
		return
	}

	err = os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		http.Error(w, "failed to create data directory", http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(dataDir, blockID)
	err = ioutil.WriteFile(filePath, data, os.ModePerm)
	if err != nil {
		http.Error(w, "failed to write data", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Data block %s written successfully", blockID)
}

// handleRead retrieves the data block from a file.
func handleRead(w http.ResponseWriter, r *http.Request) {
	blockID := r.URL.Query().Get("blockID")
	if blockID == "" {
		http.Error(w, "blockID missing", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(dataDir, blockID)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		http.Error(w, "failed to read data", http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func main() {
	http.HandleFunc("/write", handleWrite)
	http.HandleFunc("/read", handleRead)

	fmt.Println("DataNode running on port 8081...")
	http.ListenAndServe(":8081", nil)
}
