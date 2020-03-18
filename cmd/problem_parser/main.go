package main

import (
	"fmt"
	"kafka-log-processor/configs"
	"kafka-log-processor/pkg/kafka"
	"kafka-log-processor/pkg/parsers"
	"log"
)

func main() {
	config, err := configs.GetParserConfig("./configs/parser_config.yml")
	if err != nil {
		log.Fatalln(err)
	}

	kafkaService := kafka.NewProblemTopicKafka(config.Kafka.Host, config.Kafka.Port)

	for {
		eventLog, err := kafkaService.NextMessage()
		if err != nil {
			log.Println("Cannot read next message from Kafka")
			log.Println(err)
			continue
		}

		problemEvent, err := parsers.ParseProblemEvent(eventLog)
		if err != nil {
			log.Println("Cannot parse message from Kafka")
			log.Println(err)
			continue
		}

		fmt.Println(problemEvent)
	}
}
