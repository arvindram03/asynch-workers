package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/arvindram03/asynch-workers/data"
	"github.com/arvindram03/asynch-workers/rabbitmq"
	"github.com/robfig/config"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

var (
	Config *config.Config
	ENV    string
)

type Log struct {
	ID      bson.ObjectId `bson:"_id,omitempty"`
	Hour    string
	Metrics data.Metric
}

func loadConfig() (err error) {
	Config, err = config.ReadDefault("app.conf")
	return err
}

func setENV() {
	ENV = "DEV"
}

func initMongoDB() (*mgo.Session, error) {
	mongoURL, _ := Config.String(ENV, "mongo-url")
	return mgo.Dial(mongoURL)

}
func getHour(t time.Time) string {
	time := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	return time.String()
}
func processLog(metric data.Metric, session *mgo.Session) error {
	hour := getHour(time.Now().UTC())
	metricLog := &Log{Hour: hour, Metrics: metric}

	mongoDBName, _ := Config.String(ENV, "mongo-db-name")
	mongoCollectionName, _ := Config.String(ENV, "mongo-collection-name")
	c := session.DB(mongoDBName).C(mongoCollectionName)

	err := c.Insert(metricLog)
	if err != nil {
		log.Fatalf("Failed to insert log. ERR: %+v", err)
	}
	log.Printf("Metric %+v", metric)
	return err
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

	logq, _ := Config.String(ENV, "logq")
	q, err := rabbitmq.Queue(logq, ch)
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

	session, err := initMongoDB()
	if err != nil {
		log.Fatalf("Failed to start mongodb connection. ERR: %+v", err)
	}
	defer session.Close()
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			var metric data.Metric
			err := json.Unmarshal(d.Body, &metric)
			if err != nil {
				log.Fatalf("Error unmarshalling metric. ERR: %+v", err)
			}
			err = processLog(metric, session)
			if err != nil {
				d.Nack(true, true)
			}
			d.Ack(false)
		}
		fmt.Println("end")
	}()
	log.Printf("Waiting for metrics....")
	<-forever
}
