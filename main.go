package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/robfig/config"
	"github.com/streadway/amqp"
)

const (
	POST = "POST"
)

var (
	Config *config.Config
	ENV    string
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
	rabbitmqUrl, _ := Config.String(ENV, "rabbitmq-url")
	conn, err := amqp.Dial(rabbitmqUrl)
	if err != nil {
		log.Fatalf("Failed to get connection. ERR: %+v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel. ERR: %+v", err)
	}
	defer ch.Close()
	ch.Confirm(false)
	ack, nack := ch.NotifyConfirm(make(chan uint64, 1),
		make(chan uint64, 1))
	exchange, _ := Config.String(ENV, "exchange")
	err = ch.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare an exchange. ERR: %+v", err)
	}

	decoder := json.NewDecoder(r.Body)
	var metric Metric
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

	err = ch.Publish(
		exchange, // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(metricJson),
		})
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

func loadConfig() (err error) {
	Config, err = config.ReadDefault("app.conf")
	return err
}

func setENV() {
	ENV = "DEV"
}

func main() {
	setENV()
	loadConfig()
	http.HandleFunc("/metric", metricHandler)
	http.HandleFunc("/", handler)
	fmt.Println("Listening on 6055...")
	http.ListenAndServe(":6055", nil)
}
