package main

import (
	"encoding/json"
	"fmt"
	"kafka-log-processor/configs"
	"kafka-log-processor/pkg/analysers"
	"kafka-log-processor/pkg/database"
	"log"
	"net/http"
)

func main() {
	config, err := configs.GetParserConfig("./configs/parser_config.yml")
	if err != nil {
		log.Fatalln(err)
	}

	es := database.ElasticService{}
	err = es.Connect(config.Elastic.Host, config.Elastic.Port)
	if err != nil {
		log.Println("Cannot connect to ElasticSearch")
		log.Fatalln(err)
	}

	analysis, err := analysers.New(config)
	if err != nil {
		log.Fatalln(err)
	}

	courseIDsWithLogsAndStructuresHandle := GetCourseIDsHandleFunction(&es)
	usersRoutesCurversHandle := GetUsersRoutesCurves(*analysis)

	http.HandleFunc("/course-ids-with-logs-and-structs", courseIDsWithLogsAndStructuresHandle)
	http.HandleFunc("/course-routes", usersRoutesCurversHandle)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// GetCourseIDsHandleFunction generates function that writes response of course ids with logs and structures
func GetCourseIDsHandleFunction(es *database.ElasticService) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		setupResponse(&w, r)
		if (*r).Method == "OPTIONS" {
			return
		}
		courseIDs, err := es.GetAllCourseIDsWithStructureAndLogs()
		if err != nil {
			log.Fatal(err)
		}

		b, err := json.Marshal(courseIDs)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(w, string(b))
	}
}

func GetUsersRoutesCurves(analysis analysers.Analyser) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		setupResponse(&w, r)
		if (*r).Method == "OPTIONS" {
			return
		}
		course := r.URL.Query()["course"]
		fmt.Println(course[0])
		fmt.Println(course)
		points, err := analysis.GetCourseUsersRoute(course[0])
		if err != nil {
			log.Println(err)
			return
		}
		b, err := json.Marshal(points)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Fprintf(w, string(b))
	}
}

func setupResponse(w *http.ResponseWriter, req *http.Request) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	(*w).Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
}
