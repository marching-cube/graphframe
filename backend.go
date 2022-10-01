package main

import (
	"errors"
)

type Backend interface {
	ExecuteQuery(spec QuerySpec, keys [][]string) (DataFrame, error)
}

type DummyBackend struct {
	Map map[string]DataFrame
}

func CreateDummyBackend() DummyBackend {
	return DummyBackend{Map: map[string]DataFrame{}}
}

func (b *DummyBackend) AddCSVFile(name, text string) *DummyBackend {
	b.Map[name] = ReadCSV(text)
	return b
}

func (b DummyBackend) ExecuteQuery(spec QuerySpec, keys [][]string) (DataFrame, error) {
	df, ok := b.Map[spec.TableName]
	if !ok {
		return DataFrame{}, errors.New("not found " + spec.TableName)
	}
	return df.Query(spec.Key, keys), nil
}

