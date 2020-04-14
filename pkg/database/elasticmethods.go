package database

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
