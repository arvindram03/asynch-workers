package rabbitmq

import "github.com/streadway/amqp"

func Dial(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func Channel(conn *amqp.Connection) (*amqp.Channel, error) {
	return conn.Channel()
}

func GetAckNack(ch *amqp.Channel) (chan uint64, chan uint64) {
	return ch.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))
}

func Exchange(exchange string, ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
}

func PublishJson(content []byte, exchange string, ch *amqp.Channel) error {
	return ch.Publish(
		exchange, // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        content,
		})
}

func Queue(name string, ch *amqp.Channel) (*amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	return &q, err
}

func QueueBind(q *amqp.Queue, exchange string, ch *amqp.Channel) error {
	return ch.QueueBind(
		q.Name,   // queue name
		"",       // routing key
		exchange, // exchange
		false,
		nil)
}

func Consume(q *amqp.Queue, ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
}
