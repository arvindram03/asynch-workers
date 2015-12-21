# Scalable HTTP API with Async Workers using RabbitMQ in Golang
### Design
1. The server receives a HTTP request
2. Send the requests to RabbitMQ exchange (Persistent, Fanout)
3. The exchange replicates the requests into three queues `nameq, logq, accq`
4. The workers dequeues the requests from the corresponding queue
5. All the workers are scalable horizontally and the requests are distributed in round robin fashion
6. Fault Tolerance is guaranteeed by using the ACK/ NACK mechanism in rabbitmq queues. The request is removed from the queue only when it receives a ACK from the worker
7. Distributed locks are maintained as a key in Redis (WATCH and SET mechanism)

#### Getting Started
Install RabbitMQ, PostgreSQL, Redis, MongoDB and start the servers

##### Account Aggregator
`godep get github.com/arvindram03/asynch-workers/account_aggregator`

`go run account_aggregator.go`

##### Event Aggregator
`godep get github.com/arvindram03/asynch-workers/event_aggregator`

`go run event_aggregator.go`

##### Log Aggregator
`godep get github.com/arvindram03/asynch-workers/log_aggregator`

`go run log_aggregator.go`

##### HTTP Server
`godep get github.com/arvindram03/asynch-workers`

`go run main.go` and open http://localhost:6055/

Post the metric concurrently to the following url http://localhost:6055/metric

`
{
  "username": "kodingbot",
  "count": 10,
  "metric": "byte_call"
}
`