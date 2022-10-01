package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func Contains[K comparable](l []K, a K) bool {
	for _, x := range l {
		if x == a {
			return true
		}
	}
	return false
}

func ToString(v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}

	switch v_ := v.(type) {
	case string:
		return v_, nil
	case int:
		return strconv.Itoa(v_), nil
	case int64:
		return strconv.Itoa(int(v_)), nil
	case float64:
		return fmt.Sprintf("%f", v_), nil
	default:
		return "", errors.New("cannot convert ToString")
	}
}

func ToFloat64(v interface{}) (float64, error) {
	if v == nil {
		return 0, nil
	}

	switch v_ := v.(type) {
	case string:
		return strconv.ParseFloat(v_, 64)
	case int:
		return float64(v_), nil
	case int64:
		return float64(v_), nil
	case float64:
		return v_, nil
	default:
		return 0, errors.New("cannot convert ToString")
	}
}

func MapConcat(l map[string]interface{}, r map[string]interface{}) map[string]interface{} {
	n := map[string]interface{}{}
	for k, v := range l {
		n[k] = v
	}
	for k, v := range r {
		n[k] = v
	}
	return n
}

func CreateKeyCode(m map[string]interface{}, key []string) (string, []string) {
	values := []string{}
	for _, name := range key {
		// TODO: what about nulls?
		value, err := ToString(m[name])
		if err != nil {
			return "", nil
		}
		values = append(values, value)
	}
	return strings.Join(values, "!#!"), values
}

func CopyMap[K comparable, V any](m map[K]V) map[K]V {
	m2 := map[K]V{}
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

func MapEqualByKeys[K comparable](m1 map[K]any, m2 map[K]any, keys []K) bool {
	for _, key := range keys {
		if m1[key] != m2[key] {
			return false
		}
	}
	return true
}
