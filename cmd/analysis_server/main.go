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
		log.Panicf("can't connect to ElasticSearch: %v", err)
	}

	analysis, err := analysers.New(config)
	if err != nil {
		log.Fatalln(err)
	}

	courseIDsWithLogsAndStructuresHandle := GetCourseIDsHandleFunction(&es)
	usersRoutesCurversHandle := GetUsersRoutesCurves(*analysis)
	usersWatchingsCurveHandle := GetUsersWatchingCurve(*analysis)
	videoCatalogueByCourseHandle := GetVideoCatalogueByCourseHandle(&es)

	http.HandleFunc("/course-ids-with-logs-and-structs", courseIDsWithLogsAndStructuresHandle)
	http.HandleFunc("/course-routes", usersRoutesCurversHandle)
	http.HandleFunc("/users-watchings", usersWatchingsCurveHandle)
	http.HandleFunc("/video-ids-by-course", videoCatalogueByCourseHandle)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// GetUsersWatchingCurve returns points for video watching curve
func GetUsersWatchingCurve(analysis analysers.Analyser) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		setupResponse(&w, r)
		if (*r).Method == "OPTIONS" {
			return
		}
		videoID := r.URL.Query()["video_id"]
		points, err := analysis.GetAnalyseUserVideoWatchings(videoID[0])
		fmt.Println(videoID[0])
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

// GetVideoCatalogueByCourseHandle returns video ids of the course videos
func GetVideoCatalogueByCourseHandle(es *database.ElasticService) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		setupResponse(&w, r)
		if (*r).Method == "OPTIONS" {
			return
		}
		course := r.URL.Query()["course"]
		videos, err := es.GetUniqueStringFieldValuesInIndexWithFilter(
			database.VideoEventDescriptionIndexName,
			"video_id",
			"course_id",
			course[0],
		)
		if err != nil {
			log.Printf("Error! Can't get unique videos for course: %v\n", err)
			return
		}

		b, err := json.Marshal(videos)
		if err != nil {
			log.Printf("Error! Can't convert video list to json: %v\n", err)
		}
		fmt.Fprintf(w, string(b))
	}
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

// GetUsersRoutesCurves returns users routes curves
func GetUsersRoutesCurves(analysis analysers.Analyser) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		setupResponse(&w, r)
		if (*r).Method == "OPTIONS" {
			return
		}
		course := r.URL.Query()["course"]
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
