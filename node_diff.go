package main

import (
	"errors"
	"fmt"
	"strconv"
)

type DiffNodeAction struct {
	name        string
	source      string
	fn          func(a any, b any, state any) (any, any)
	default_    interface{}
	partitionby []string
}

func (g *Graph) createDiffNode(name string, source string, fn string, partitionby []string, default_ interface{}, label string) (GraphNode, error) {
	var fn_ func(a any, b any, state any) (any, any)
	switch fn {
	case "diff", "", "default":
		fn_ = fnDiff
	case "rank", "rnk":
		fn_ = fnRank
	case "number", "id", "index":
		fn_ = fnNumber
	case "lag":
		fn_ = fnLag
	case "lead":
		fn_ = fnLead
	default:
		return GraphNode{}, errors.New("we do not support differentiation operator: " + fn)
	}

	return GraphNode{
		label:        label,
		fields:       []string{name},
		dependencies: []GraphDependency{{Node: g.nodes[len(g.nodes)-1]}},
		write:        make(chan DataFrameList),
		action:       DiffNodeAction{name: name, source: source, fn: fn_, partitionby: partitionby, default_: default_},
	}, nil
}

func (n DiffNodeAction) Name() string {
	return "diff"
}

func (n DiffNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {
	defer close(output)

	readPair := func(df DataFrame, idx int) (any, any) {
		a := df.Rows[idx][n.source]
		var b interface{} = nil
		if idx+1 < len(df.Rows) && MapEqualByKeys(df.Rows[idx+1], df.Rows[idx], n.partitionby) {
			b = df.Rows[idx+1][n.source]
		}
		return a, b
	}

	for {
		row, more := <-input
		if !more {
			break
		}
		df := row.Flatten()
		var state interface{} = n.default_
		nrows := []map[string]interface{}{}
		for idx := 0; idx < len(df.Rows); idx++ {
			nrow := CopyMap(df.Rows[idx])
			a, b := readPair(df, idx)
			nrow[n.name], state = n.fn(a, b, state)
			if nrow[n.name] == nil {
				nrow[n.name] = n.default_
			}
			nrows = append(nrows, nrow)
			if b == nil {
				state = nil
			}
		}

		output <- DataFrameList{DataFrame{
			Header: append(df.Header, n.name),
			Rows:   nrows,
			Index:  df.Index,
		}}
	}
}

func fnDiff(a any, b any, state any) (any, any) {
	if a == nil {
		return nil, state
	}
	if b == nil {
		return nil, state
	}
	switch v := a.(type) {
	case int:
		return b.(int) - v, state
	case int64:
		return b.(int64) - v, state
	case float64:
		return b.(float64) - v, state
	case float32:
		return b.(float32) - v, state
	case string:
		av, err := strconv.ParseFloat(a.(string), 64)
		if err != nil {
			panic(err) // TODO
		}
		bv, err := strconv.ParseFloat(b.(string), 64)
		if err != nil {
			panic(err) // TODO
		}
		return bv - av, state

	default:
		panic(fmt.Errorf("could not diff this type of data: %v", v)) // TODO
	}
}

func fnLag(a any, b any, state any) (any, any) {
	return state, a
}

func fnLead(a any, b any, state any) (any, any) {
	return b, state
}

func fnNumber(a any, b any, state any) (any, any) {
	if state == nil {
		return 0, 0
	}
	return state.(int) + 1, state.(int) + 1
}

func fnRank(a any, b any, state any) (any, any) {
	if state == nil {
		state = 0
	}
	if a == b {
		return state, state
	}
	return state, state.(int) + 1
}
