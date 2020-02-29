package helpers

import (
	"fmt"
)

// ExtractStringFieldFromGenericMap checks if field is string and exctracts it as a string
func ExtractStringFieldFromGenericMap(mapObject map[string]interface{}, fieldName string) (string, error) {
	fieldObject, ok := mapObject[fieldName]
	if !ok {
		return "", fmt.Errorf("no %v in the object", fieldName)
	}

	fieldValue, ok := fieldObject.(string)
	if !ok {
		return "", fmt.Errorf("%v is not a string", fieldName)
	}

	return fieldValue, nil
}
