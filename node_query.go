package main

import (
	"errors"
	"strings"
)

type QueryNodeAction struct {
	query QuerySpec
}

func (n QueryNodeAction) Name() string {
	return "query"
}

func (n QueryNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {

	defer close(output)

	// process incoming messages
	for {
		msg, more := <-input
		if !more {
			break
		}
		// TODO
		//		(not now) merge multiple sources if needed
		//		(not now) aggregate or split messages
		queryKeys := uniqueQueryKeys(n.query, msg)
		data, err := backend.ExecuteQuery(n.query, queryKeys) // no go
		data.Optional = n.query.Optional
		if err != nil {
			// TODO: !!!!
		}
		output <- append(msg, data)
	}
}

func (g *Graph) findQueryDependencies(query QuerySpec) ([]GraphDependency, error) {
	dependencies := []GraphDependency{}
	for _, node := range g.nodes {
		matched := []string{}
		for _, akey := range query.Key {
			if Contains(node.fields, akey) {
				matched = append(matched, akey)
			}
		}
		if len(matched) > 0 {
			dependencies = append(dependencies, GraphDependency{
				Node: node,
				Key:  matched,
			})
			if len(matched) != len(query.Key) {
				// TODO: implement partial key matching
				return dependencies, errors.New("partial key matching is not supported, yet")
			}
		}
	}
	return dependencies, nil
}

func (g *Graph) createQueryNode(query QuerySpec, label string) (GraphNode, error) {

	dependencies, err := g.findQueryDependencies(query)
	if err != nil {
		return GraphNode{}, err
	}

	// throw if key not matched (key can be empty!!!)
	if len(query.Key) > 0 && len(dependencies) == 0 {
		return GraphNode{}, errors.New("no way to perform Query join on " + strings.Join(query.Key, ","))
	}

	//create a new node (with spec, dependencies)
	return GraphNode{
		label:        label,
		fields:       query.Fields,
		dependencies: dependencies,
		write:        make(chan DataFrameList),
		action:       QueryNodeAction{query: query},
	}, nil
}

func uniqueQueryKeys(query QuerySpec, msg DataFrameList) [][]string {
	c := map[string]bool{}

	data := msg[len(msg)-1] // remark: join on a dependency

	queryKeys := [][]string{}
	for _, row := range data.Rows {
		ckey, keyValue := CreateKeyCode(row, query.Key)
		if _, ok := c[ckey]; !ok {
			c[ckey] = true
			queryKeys = append(queryKeys, keyValue)
		}
	}
	return queryKeys
}
