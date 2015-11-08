package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/arvindram03/asynch-workers/data"
	"github.com/arvindram03/asynch-workers/rabbitmq"
	"github.com/robfig/config"
)

const (
	POST = "POST"
)

var (
	Config *config.Config
	ENV    string
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello Gophers :)")
}

func metricHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != POST {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	rabbitmqUrl, _ := Config.String(ENV, "rabbitmq-url")
	conn, err := rabbitmq.Dial(rabbitmqUrl)
	if err != nil {
		log.Fatalf("Failed to get connection. ERR: %+v", err)
	}
	defer conn.Close()

	ch, err := rabbitmq.Channel(conn)
	if err != nil {
		log.Fatalf("Failed to open channel. ERR: %+v", err)
	}
	defer ch.Close()
	ch.Confirm(false)

	ack, nack := rabbitmq.GetAckNack(ch)
	exchange, _ := Config.String(ENV, "exchange")
	err = rabbitmq.Exchange(exchange, ch)
	if err != nil {
		log.Fatalf("Failed to declare an exchange. ERR: %+v", err)
	}

	decoder := json.NewDecoder(r.Body)
	var metric data.Metric
	err = decoder.Decode(&metric)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	metricJson, err := json.Marshal(metric)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = rabbitmq.PublishJson(metricJson, exchange, ch)
	if err != nil {
		log.Fatalf("Failed to publish message. ERR: %+v", err)
	}

	select {
	case <-ack:
		log.Printf("Pushed. Metric: %+v\n", metric)
		w.WriteHeader(http.StatusOK)
	case <-nack:
		log.Printf("Failed tp push. Metric: %+v\n", metric)
		w.WriteHeader(http.StatusInternalServerError)
	}

}

func setENV() {
	ENV = "DEV"
}

func loadConfig() {
	var err error
	Config, err = config.ReadDefault("app.conf")
	if err != nil {
		log.Fatalf("Error loading config. ERR: %+v", err)
	}
}

func main() {
	setENV()
	loadConfig()
	http.HandleFunc("/metric", metricHandler)
	http.HandleFunc("/", handler)
	fmt.Println("Listening on 6055...")
	http.ListenAndServe(":6055", nil)
}
