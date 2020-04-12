package main

import (
	"io/ioutil"
	"kafka-log-processor/configs"
	"log"
	"os"
	"time"

	"kafka-log-processor/pkg/database"

	edxstruct "github.com/veotani/edx-structure-json"
)

func getUnparsedStructureFileName() string {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		fname := f.Name()
		if len(fname) < 7 {
			continue
		}
		if fname[len(fname)-7:len(fname)] == ".tar.gz" {
			return fname
		}
	}

	return ""
}

func main() {
	config, err := configs.GetParserConfig("./configs/parser_config.yml")
	if err != nil {
		log.Fatalln(err)
	}

	es := database.ElasticService{}
	if err = es.Connect(config.Elastic.Host, config.Elastic.Port); err != nil {
		log.Fatal(err)
	}

	for {
		fname := getUnparsedStructureFileName()
		if fname == "" {
			time.Sleep(time.Second * 3)
			continue
		}

		course, err := edxstruct.ParseCourse(fname)
		if err != nil {
			log.Println(err)
			os.Remove(fname)
			continue
		}

		if err = es.AddCourseStructure(course); err != nil {
			log.Println(err)
		}

		os.Remove(fname)
	}
}
