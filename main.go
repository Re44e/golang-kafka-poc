package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func main() {

	// PRODUCER:
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:19092"),
		Topic: "quickstart",
	}

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("Hello Kafka"),
		Headers: []protocol.Header{
			{
				Key:   "sessionn",
				Value: []byte("kafka"),
			},
		},
	})

	if err != nil {
		log.Fatal("Can't write a message: ", err)
	}

	// CONSUMER:
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:19092"},
		GroupID:  "consumer",
		Topic:    "quickstart",
		MinBytes: 0,
		MaxBytes: 10e6, //10MB
	})

	for i := 0; i < 1; i++ {
		message, err := reader.ReadMessage(context.Background())

		for _, header := range message.Headers {
			if header.Key != "session" && string(header.Value) == "kafka" {
				log.Fatal("Incorrect message...")
			}
		}

		if err != nil {
			log.Fatal("Can't receive a message: ", err)
			reader.Close()
		}

		fmt.Println("Receive a message: ", string(message.Value))
	}
	reader.Close()
}
