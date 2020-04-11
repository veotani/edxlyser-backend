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

	kafkaService := kafka.NewLinksTopicKafka(config.Kafka.Host, config.Kafka.Port)

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

		linksEvent, err := parsers.ParseLinkEvent(eventLog)
		if err != nil {
			log.Println("Cannot parse links log message from Kafka")
			log.Println(err)
			continue
		}

		err = elastic.AddLinkEventDescription(linksEvent)
		if err != nil {
			log.Println("Cannot save parsed links log in ElasticSearch")
			log.Println(err)
		}
	}
}
