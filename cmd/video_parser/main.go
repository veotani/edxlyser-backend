package main

import (
	"kafka-log-processor/configs"
	"kafka-log-processor/pkg/database"
	"kafka-log-processor/pkg/kafka"
	"kafka-log-processor/pkg/parsers"
	"log"
)

func main() {
	config, err := configs.GetParserConfig("./configs/parser_config.yml")
	if err != nil {
		log.Fatalln(err)
	}

	kafkaService := kafka.NewVideoTopicKafka(config.Kafka.Host, config.Kafka.Port)

	elastic := database.ElasticService{}
	err = elastic.Connect(config.Elastic.Host, config.Elastic.Port)
	if err != nil {
		log.Panicln("Cannot connect to ElasticSearch")
		log.Fatalln(err)
	}

	for {
		eventLog, err := kafkaService.NextMessage()
		if err != nil {
			log.Println("Cannot read next message from Kafka")
			log.Println(err)
			continue
		}

		videoEvent, err := parsers.ParseVideoEvent(eventLog)
		if err != nil {
			log.Println("Cannot parse message from Kafka")
			log.Println(err)
			continue
		}

		err = elastic.AddVideoEventDescription(videoEvent)
		if err != nil {
			log.Println("Cannot save parsed log in ElasticSearch")
			log.Println(err)
		}
	}
}
