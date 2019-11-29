package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
)

const inputTopic = "Generated"

func main() {
	// подписчик очереди Kafka (consumer)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   inputTopic,
		// GroupID:   "consumer-group-id-3",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e7, // 10MB
	})

	var wg sync.WaitGroup
	c := 0 //counter

	for {
		// создайм объект контекста с таймаутом в 15 секунд для чтения сообщений
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// читаем очередное сообщение из очереди
		// поскольку вызов блокирующий - передаём контекст с таймаутом
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("3")
			fmt.Println(err)
		}

		wg.Add(1)
		// создайм объект контекста с таймаутом в 10 миллисекунд для каждой вычислительной горутины
		_, goCcancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer goCcancel()

		// вызываем функцию обработки сообщения (факторизации)
		fmt.Println(string(m.Value))
		c++
	}
	// ожидаем завершения всех горутин
	wg.Wait()
}
