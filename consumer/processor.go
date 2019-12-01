package main

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

const inputTopic = "TestEvents"

func main() {
	// Kafka consumer
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     inputTopic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e7, // 10MB
	})

	// Open file to write to (0222 FileMode stands for write only unix permissions)
	// https://en.wikipedia.org/wiki/File_system_permissions
	// f, err := os.OpenFile("output.txt", os.O_APPEND|os.O_WRONLY, 0222)
	// if err != nil {
	// 	fmt.Println("cannot open file to write to")
	// 	fmt.Println(err)
	// }

	// defer f.Close()

	// countWrites := 0
	// fmt.Println("starting writting kafka logs to file")
	for {
		ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(string(m.Value))
		break

		// fmt.Println(string(m.Value))
		// n, err := f.WriteString(string(m.Value) + "\n")
		// check(err)
		// fmt.Printf("success!\n%v\n", n)
		// countWrites++
		// if countWrites == 10 {
		// 	f.Sync()
		// 	break
		// }

		// var obj interface{}
		// json.Unmarshal(m.Value, &obj)
		// decodedObject := obj.(map[string]interface{})
		// fmt.Println(eventType)
		// f.WriteString(eventType)
		// fmt.Println(decodedObject)
		// f.WriteString(string(m.Value))
		// break
		// for k, v := range decodedObject {
		// 	// switch vv := v.(type) {
		// 	if k == "event_type" {
		// 		fmt.Println(v)
		// 	}
		// switch v.(type) {
		// case string:
		// 	fmt.Println(k, "is string")
		// }
		// }
	}
}
