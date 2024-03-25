package utils

import (
	"encoding/json"
	"errors"
	"reflect"
)

func ParseMap[T any](object any, out *T) error {

	bytes, err := json.Marshal(object)
	if err != nil {
		return err
	}

	var temp T
	err = json.Unmarshal(bytes, &temp)
	if err != nil {
		return err
	}
	*out = temp
	return nil
}

func ParseMapKey[T any](object map[string]any, key string, out *T) error {
	field, exists := object[key]
	if !exists {
		return errors.New("key " + key + " doesn't exists in map")
	}

	bytes, err := json.Marshal(object[key])
	if err != nil {
		return err
	}

	var temp T
	err = json.Unmarshal(bytes, &temp)
	if err != nil {
		return errors.New("type for " + key + " mismatch " + reflect.TypeOf(field).String())
	}
	*out = temp
	return nil
}
