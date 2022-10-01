package main

import (
	"errors"
	"strconv"
	"strings"
)

type FilterSpec struct {
	leftValue     interface{}
	leftVariable  string
	op            FilterOpType
	rightValue    interface{}
	rightVariable string
}

type FilterOpType int

const (
	FilterOpUnknown FilterOpType = iota
	FilterOpEqual
	FilterOpLess
	FilterOpLessOrEqual
	FilterOpMore
	FilterOpMoreOrEqual
	FilterOpNotEqual
)

func createFilterOpType(text string) FilterOpType {
	switch text {
	case "==":
		return FilterOpEqual
	case "<":
		return FilterOpLess
	case "<=":
		return FilterOpLessOrEqual
	case ">":
		return FilterOpMore
	case ">=":
		return FilterOpMoreOrEqual
	case "!=":
		return FilterOpNotEqual
	default:
		return FilterOpUnknown
	}
}

func createFilterValue(text string) (interface{}, error) {
	if strings.HasPrefix(text, "$") {
		return nil, nil
	} else if strings.HasPrefix(text, "\"") {
		return text[1 : len(text)-2], nil
	} else if strings.Contains(text, ".") {
		return strconv.ParseFloat(text, 64)
	} else {
		return strconv.Atoi(text)
	}
}

func createFilterSpec(text string) (FilterSpec, error) {
	items := strings.Split(strings.ReplaceAll(strings.TrimSpace(text), "  ", " "), " ")
	if len(items) != 3 {
		return FilterSpec{}, errors.New("invalid filter definition")
	}

	var leftValue interface{}
	var leftVariable string
	var rightValue interface{}
	var rightVariable string

	if v, err := createFilterValue(items[0]); err == nil {
		leftValue = v
	}
	if strings.HasPrefix(items[0], "$") {
		leftVariable = items[0][1:]
	}
	if v, err := createFilterValue(items[2]); err == nil {
		rightValue = v
	}
	if strings.HasPrefix(items[2], "$") {
		rightVariable = items[0][1:]
	}

	return FilterSpec{
		leftValue:     leftValue,
		leftVariable:  leftVariable,
		op:            createFilterOpType(items[1]),
		rightValue:    rightValue,
		rightVariable: rightVariable,
	}, nil
}

func (f FilterSpec) CheckRow(row map[string]interface{}) bool {
	leftValue := f.leftValue
	if f.leftVariable != "" {
		leftValue = row[f.leftVariable]
	}
	rightValue := f.rightValue
	if f.rightVariable != "" {
		rightValue = row[f.rightVariable]
	}
	
	switch f.op {
	case FilterOpEqual:
		return leftValue == rightValue
	case FilterOpLess:
		return less(leftValue, rightValue)
	case FilterOpLessOrEqual:
		return less(leftValue, rightValue) || (leftValue == rightValue)
	case FilterOpMore:
		return !less(leftValue, rightValue) && (leftValue != rightValue)
	case FilterOpMoreOrEqual:
		return !less(leftValue, rightValue)
	case FilterOpNotEqual:
		return leftValue != rightValue
	default:
		return false
	}
}

func (f FilterSpec) Filter(df DataFrame) DataFrame {
	rows := []map[string]interface{}{}
	for _, row := range df.Rows {
		if f.CheckRow(row) {
			rows = append(rows, row)
		}
	}
	return DataFrame{
		Rows: rows,
		Index: df.Index,
		Header: df.Header,
		Optional: df.Optional,
	}
}

func less(a interface{}, b interface{}) bool {
	switch v := a.(type) {
	case int:
		return v < b.(int)
	case int64:
		return v < b.(int64)
	case float32:
		return v < b.(float32)
	case float64:
		return v < b.(float64)
	case string:
		return v < b.(string)
	default:
		panic("!!!")
	}
}

func (f FilterSpec) Variables() []string {
	result := make([]string, 0)
	if f.leftVariable != "" {
		result = append(result, f.leftVariable)
	}
	if f.rightVariable != "" {
		result = append(result, f.rightVariable)
	}
	return result
}