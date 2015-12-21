package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/arvindram03/asynch-workers/data"
	"github.com/arvindram03/asynch-workers/rabbitmq"
	"github.com/robfig/config"
	redis "gopkg.in/redis.v3"
)

const (
	DIST_LOCK = "DIST_LOCK"
	ACQUIRED  = 1
)

var (
	Config *config.Config
	ENV    string
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

func initRedisClient() *redis.Client {
	redisUrl, _ := Config.String(ENV, "redis-url")
	return redis.NewClient(&redis.Options{
		Addr:     redisUrl,
		Password: "",
		DB:       0,
	})
}

func getEvents(keys []string) (events []string) {
	for _, key := range keys {
		parts := strings.Split(key, " ")
		if len(parts) != 2 {
			continue
		}
		event := parts[1]
		events = append(events, event)
	}
	return
}

func aggregate(client *redis.Client, year int, month int) error {
	log.Println("Curating logs...")
	yearMonth := strconv.Itoa(year) + "-" + strconv.Itoa(month)
	key := yearMonth + "-*"
	keys, err := client.Keys(key).Result()
	if err != nil {
		log.Printf("Failed to set metric connection. ERR: %+v", err)
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	events := getEvents(keys)
	monthlyEvent := struct {
		Events []string
	}{
		events,
	}

	byteContent, err := json.Marshal(monthlyEvent)
	if err != nil {
		log.Println("Failed to set all event under single key. ERR: %+v", err)
		return err
	}

	err = client.Set(yearMonth, byteContent, 0).Err()
	if err != nil {
		log.Println("Failed to set all event under single key. ERR: %+v", err)
		return err
	}

	err = client.Del(keys...).Err()
	if err != nil {
		log.Println("Failed to delete all event in the past month. ERR: %+v", err)
		return err
	}
	return err
}

func acquireLock(client *redis.Client) bool {
	_, err := client.Watch(DIST_LOCK)
	if err != nil {
		log.Fatalf("Error watching distributed lock key. ERR: %+v", err)
		return false
	}
	value, err := client.Get(DIST_LOCK).Int64()
	if err != nil {
		log.Fatalf("Error getting distributed lock key. ERR: %+v", err)
		return false
	}
	if value == ACQUIRED {
		return false
	}
	multi := client.Multi()
	_, err = multi.Exec(func() error {
		err := multi.Set(DIST_LOCK, ACQUIRED, 0).Err()
		if err != nil {
			log.Fatalf("Error setting distributed lock key. ERR: %+v", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error executing the set distributed lock key. ERR: %+v", err)
		return false
	}
	return true
}

func releaseLock(client *redis.Client) {
	err := client.Set(DIST_LOCK, 0, 0).Err()
	if err != nil {
		log.Fatalf("Error in releasing the lock. ERR: %+v", err)
	}
}

func curate() {
	client := initRedisClient()
	if !acquireLock(client) {
		return
	}
	fmt.Println("Acquired Lock")
	year, month, _ := time.Now().UTC().Date()
	retryCount, _ := Config.Int(ENV, "retry-count")
	backoff_time := 2 * time.Second
	for i := 0; i < retryCount; i++ {
		err := aggregate(client, year, int(month))
		if err == nil {
			break
		}
		log.Println("Failed to aggregate logs for the month. ERR: %+v", err)
		backoff_time = backoff_time * 2
		log.Println("Backing off for", backoff_time)
		<-time.After(backoff_time)
	}
	releaseLock(client)
	client.Close()
}

func getEndOfMonth() time.Duration {
	now := time.Now()
	return time.Since(time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC))
}

func curateLogs() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			go curateLogs()
		}
	}()
	<-time.After(getEndOfMonth())
	curate()
	c := time.Tick(30 * 24 * time.Hour)
	// c := time.Tick(3 * time.Second)
	for _ = range c {
		curate()
	}
}

func process(metric data.Metric, client *redis.Client) error {
	year, month, day := time.Now().UTC().Date()
	date := strconv.Itoa(year) + "-" + strconv.Itoa(int(month)) + "-" + strconv.Itoa(day)
	key := date + " " + metric.Metric
	err := client.Set(key, true, 0).Err()
	if err != nil {
		log.Fatalf("Failed to set metric connection. ERR: %+v", err)
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

	nameq, _ := Config.String(ENV, "nameq")
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
			client := initRedisClient()
			var metric data.Metric
			err := json.Unmarshal(d.Body, &metric)
			if err != nil {
				log.Fatalf("Error unmarshalling metric. ERR: %+v", err)
			}
			err = process(metric, client)
			if err != nil {
				d.Nack(true, true)
			}
			d.Ack(false)
			client.Close()
		}
	}()

	// // if curator panics, recovers and invoked the curator again after sleeping
	// // for 10 seconds
	// go func() {

	// }()
	go curateLogs()

	log.Printf("Waiting for metrics....")
	<-forever
}
