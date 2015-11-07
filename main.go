package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

const (
	POST = "POST"
)

type Metric struct {
	Username string `json:"username"`
	Count    int64  `json:"count"`
	Metric   string `json:"metric"`
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello Gopher :)")
}

func metricHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != POST {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	decoder := json.NewDecoder(r.Body)
	var metric Metric
	err := decoder.Decode(&metric)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Printf("%+v\n", metric)
}

func main() {
	http.HandleFunc("/metric", metricHandler)
	http.HandleFunc("/", handler)
	fmt.Println("Listening on 6055...")
	http.ListenAndServe(":6055", nil)
}
