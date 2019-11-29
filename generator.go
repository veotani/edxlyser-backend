package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const topic = "Generated"

func main() {
	// queue client
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for i := 1; i < 10; i++ {
		err = conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if err != nil {
			log.Fatal(err)
		}

		// write to queue
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte("babka " + strconv.Itoa(i))},
		)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Written to topic:", i)
	}
}
