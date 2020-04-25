package database

import "context"

// CreateVideoIndexIfNotExists creates index for video events
func (es *ElasticService) CreateVideoIndexIfNotExists() error {
	exists, err := es.client.IndexExists(VideoEventDescriptionIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"video_id": { "type": "keyword" },
			"video_time": { "type": "double" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := es.client.CreateIndex(VideoEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateBookmarksIndexIfNotExists creates index for booksmark events
func (es *ElasticService) CreateBookmarksIndexIfNotExists() error {
	exists, err := es.client.IndexExists(BookmarsEventDescriptionIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"id": { "type": "keyword" },
			"is_added": { "type": "binary" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := es.client.CreateIndex(BookmarsEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateLinksIndexIfNotExists creates index for link events
func (es *ElasticService) CreateLinksIndexIfNotExists() error {
	exists, err := es.client.IndexExists(LinkEventDescriptionIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"current_url": { "type": "keyword" },
			"target_url": { "type": "keyword" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := es.client.CreateIndex(LinkEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateProblemIndexIfNotExists creates index for problem events
func (es *ElasticService) CreateProblemIndexIfNotExists() error {
	exists, err := es.client.IndexExists(ProblemEventDescriptionIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"problem_id": { "type": "keyword" },
			"weighted_earned": { "type": "double" },
			"weighted_possible": { "type": "double" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := es.client.CreateIndex(ProblemEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateSequentialIndexIfNotExists creates index for sequential events
func (es *ElasticService) CreateSequentialIndexIfNotExists() error {
	exists, err := es.client.IndexExists(SequentialEventDescriptionIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"old": { "type": "integer" },
			"new": { "type": "integer" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := es.client.CreateIndex(SequentialEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateStructureIndexIfNotExists craetes index for course structures
func (es *ElasticService) CreateStructureIndexIfNotExists() error {
	exists, err := es.client.IndexExists(CourseStructureIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"course": {
				"properties": {
					"display_name": {"type": "keyword"},
					"course_code": {"type": "keyword"},
					"course_run": {"type": "keyword"},
					"chapters": {
						"properties": {
							"url_name": {"type": "keyword"},
							"display_name": {"type": "keyword"},
							"sequentials": {
								"properties": {
									"display_name": {"type": "keyword"},
									"url_name": {"type": "keyword"},
									"verticals": {
										"properties": {
											"display_name": {"type": "keyword"},
											"url_name": {"type": "keyword"},
											"problems": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"}
												}
											},
											"discussions": {
												"properties": {
													"url_name": {"type": "keyword"}
												}
											},
											"htmls": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"}
												}
											},
											"open_assessments": {
												"properties": {
													"url_name": {"type": "keyword"}
												}
											},
											"library_contents": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"},
													"problems": {
														"properties": {
															"display_name": {"type": "keyword"},
															"url_name": {"type": "keyword"}
														}
													}
												}
											},
											"videos": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"},
													"duration": {"type": "keyword"}
												}
											}
										}
									}
								}
							}
						}
					}
				} 
			}
		}
	}
}
`
		_, err := es.client.CreateIndex(CourseStructureIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	return nil
}
