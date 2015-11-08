package main

import (
	"encoding/json"
	"log"

	"github.com/robfig/config"
	"github.com/streadway/amqp"
)

type Metric struct {
	Username string `json:"username"`
	Count    int64  `json:"count"`
	Metric   string `json:"metric"`
}

var (
	Config *config.Config
	ENV    string
)

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
	logq, _ := Config.String(ENV, "logq")
	q, err := ch.QueueDeclare(
		logq,  // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue. ERR: %+v", err)
	}

	err = ch.QueueBind(
		q.Name,   // queue name
		"",       // routing key
		exchange, // exchange
		false,
		nil)
	if err != nil {
		log.Fatalf("Failed to bind. ERR: %+v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register name collector. ERR: %+v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var metric Metric
			err := json.Unmarshal(d.Body, &metric)
			if err != nil {
				log.Fatalf("Error unmarshalling metric. ERR: %+v", err)
			}
			d.Ack(false)
			log.Printf("Metric %+v", metric)
		}
	}()

	log.Printf("Waiting for metrics....")
	<-forever
}
