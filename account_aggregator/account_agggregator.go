package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/arvindram03/asynch-workers/data"
	"github.com/arvindram03/asynch-workers/rabbitmq"
	"github.com/go-gorp/gorp"
	"github.com/lib/pq"
	"github.com/robfig/config"
)

/*
CREATE TABLE accounts (
	id serial primary key,
	name text unique,
	time timestamp without time zone
);
*/
type Account struct {
	Id   int64     `db:"id"`
	Name string    `db:"name"`
	Time time.Time `db:"time"`
}

var (
	Config *config.Config
	ENV    string
)

const (
	PG_UNIQUE_VIOLATION_ERR = "unique_violation"
)

func loadConfig() (err error) {
	Config, err = config.ReadDefault("../app.conf")
	if err != nil {
		log.Fatalf("Failed to read configs. ERR: %+v", err)
	}
	return err
}

func setENV() {
	ENV = "DEV"
}

func initDb() *gorp.DbMap {
	postgresUrl, _ := Config.String(ENV, "postgres-url")
	db, err := sql.Open("postgres", postgresUrl)
	if err != nil {
		log.Fatalf("Failed to get postgres connection. ERR: %+v", err)
	}

	dbMap := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}
	dbMap.AddTableWithName(Account{}, "accounts").SetKeys(true, "Id")

	return dbMap
}

func process(metric data.Metric, dbMap *gorp.DbMap) error {
	log.Printf("Metric %+v", metric)
	now := time.Now().UTC()
	account := Account{Name: metric.Username, Time: now}

	err := dbMap.Insert(&account)
	if err, ok := err.(*pq.Error); ok &&
		err.Code.Name() != PG_UNIQUE_VIOLATION_ERR {
		log.Fatalf("Error inserting account. ERR: %+v", err)
	}

	return nil
}

func main() {
	setENV()
	loadConfig()
	dbMap := initDb()
	defer dbMap.Db.Close()

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

	accq, _ := Config.String(ENV, "accq")
	q, err := rabbitmq.Queue(accq, ch)
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

			err = process(metric, dbMap)
			if err != nil {
				d.Nack(true, true)
			}
			d.Ack(false)
		}
	}()

	log.Printf("Waiting for metrics....")
	<-forever
}
