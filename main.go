package main

import (
	"log"
	"strconv"

	"github.com/buger/jsonparser"
	amqp "github.com/rabbitmq/amqp091-go"
)

type generatorJobs struct {
	Idtrxkeluarandetail string
	Idtrxkeluaran       string
	Datetimedetail      string
	Company             string
	Username            string
	Nomortogel          string
	create              string
	createDate          string
}
type generatorResult struct {
	Idtrxkeluarandetail string
	Message             string
	Status              string
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@157.230.255.100:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"generator", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}
	// totalWorker := 100
	go func() {
		for d := range msgs {
			json := []byte(d.Body)
			jsonparser.ArrayEach(json, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				Totaldata, _, _, _ := jsonparser.Get(value, "Totaldata")
				Record, _, _, _ := jsonparser.Get(value, "Record")

				// json := []byte(d.Body)
				jsonparser.ArrayEach(Record, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
					Idtrxkeluarandetail, _, _, _ := jsonparser.Get(value, "Idtrxkeluarandetail")
					log.Printf("%s", Idtrxkeluarandetail)
				})

				total_bet, _ := strconv.Atoi(string(Totaldata))
				log.Printf("%s", total_bet)
				// buffer_bet := total_bet + 1
				// jobs_bet := make(chan generatorJobs, buffer_bet)
				// results_bet := make(chan generatorResult, buffer_bet)
				// wg := &sync.WaitGroup{}
				// for w := 0; w < totalWorker; w++ {
				// wg.Add(1)
				// go _doJobInsertTransaksi(fieldtable, jobs_bet, results_bet, wg)
				// }
			})
		}
	}()

	log.Printf(" [*] Waiting for messages generator. To exit press CTRL+C")
	<-forever
}
