package main

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
)

const inputTopic = "Generated"

func main() {
	// Kafka consumer
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   inputTopic,
		// GroupID:   "consumer-group-id-3",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e7, // 10MB
	})

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(string(m.Value))
	}
}
