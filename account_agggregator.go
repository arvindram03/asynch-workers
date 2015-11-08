package main

import (
	"encoding/json"
	"log"

	"github.com/arvindram03/asynch-workers/data"
	"github.com/arvindram03/asynch-workers/rabbitmq"
	"github.com/robfig/config"
)

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
	exchange, _ := Config.String(ENV, "exchange")
	err = rabbitmq.Exchange(exchange, ch)
	if err != nil {
		log.Fatalf("Failed to declare an exchange. ERR: %+v", err)
	}

	nameq, _ := Config.String(ENV, "accq")
	q, err := rabbitmq.Queue(nameq, ch)
	if err != nil {
		log.Fatalf("Failed to declare a queue. ERR: %+v", err)
	}

	err = rabbitmq.QueueBind(q, exchange, ch)
	if err != nil {
		log.Fatalf("Failed to bind. ERR: %+v", err)
	}

	msgs, err := rabbitmq.Consume(q, ch)
	if err != nil {
		log.Fatalf("Failed to register name collector. ERR: %+v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var metric data.Metric
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
