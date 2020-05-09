package database

// Aggregations that are used in this project

import (
	"context"
	"errors"
	"fmt"

	"github.com/olivere/elastic"
)

// GetUniqueStringFieldValuesInIndex returns all possible values of field fieldName in index indexName
// Field fieldName should have type keyword (string)
func (es *ElasticService) GetUniqueStringFieldValuesInIndex(indexName string, fieldName string) ([]string, error) {
	if es.client == nil {
		return nil, errors.New("You need to connect to ElasticSearch first")
	}
	searchResults, err := es.client.Search().
		Index(indexName).
		Aggregation("custom_aggregation", elastic.NewTermsAggregation().
			Field(fieldName)).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	aggregationResults, ok := searchResults.Aggregations.Terms("custom_aggregation")
	if !ok {
		return nil, errors.New("Nothing was found")
	}
	result := make([]string, 0)
	for _, bucket := range aggregationResults.Buckets {
		if fieldValue, ok := bucket.Key.(string); ok {
			result = append(result, fieldValue)
		} else {
			return nil, fmt.Errorf("Field %v should be a keyword", fieldName)
		}
	}
	return result, nil
}

// GetUniqueStringFieldValuesInIndexWithFilter is a GetUniqueStringFieldValuesInIndex but with ability to filter the field filteredFieldName
func (es *ElasticService) GetUniqueStringFieldValuesInIndexWithFilter(indexName string, fieldName string, filteredFieldName string, filteredFieldValue interface{}) ([]string, error) {
	if es.client == nil {
		return nil, errors.New("You need to connect to ElasticSearch first")
	}
	searchResults, err := es.client.Search().
		Index(indexName).
		Query(elastic.NewTermQuery(filteredFieldName, filteredFieldValue)).
		Aggregation("custom_aggregation", elastic.NewTermsAggregation().
			Field(fieldName).
			Size(1e8)).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	aggregationResults, ok := searchResults.Aggregations.Terms("custom_aggregation")
	if !ok {
		return nil, errors.New("Nothing was found")
	}
	result := make([]string, 0)
	for _, bucket := range aggregationResults.Buckets {
		if fieldValue, ok := bucket.Key.(string); ok {
			result = append(result, fieldValue)
		} else {
			return nil, fmt.Errorf("Field %v should be a keyword", fieldName)
		}
	}
	return result, nil
}
