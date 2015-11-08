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

func getQueue(ch *amqp.Channel, queue string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
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

	queue, _ := Config.String(ENV, "metrics")
	q, err := getQueue(ch, queue)
	if err != nil {
		log.Fatalf("Failed to open channel. ERR: %+v", err)
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
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(metricJson),
		})
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Printf("Pushed to %+v\n", metric)
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
