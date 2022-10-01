package main

import (
	"errors"
)

type MapType map[interface{}]interface{}

type MapNodeAction struct {
	name string
	m    MapType
}

func (g *Graph) createMapNode(name string, m MapType, label string) (GraphNode, error) {
	dependencies := g.findFieldDependencies(name)
	if len(dependencies) == 0 {
		g.err = errors.New("could not connect field " + name)
	}
	return GraphNode{
		label:        label,
		fields:       []string{name},
		dependencies: dependencies,
		write:        make(chan DataFrameList),
		action:       MapNodeAction{name: name, m: m},
	}, nil
}

func (g *Graph) createFillNilNode(name string, value any, label string) (GraphNode, error) {
	dependencies := g.findFieldDependencies(name)
	if len(dependencies) == 0 {
		g.err = errors.New("could not connect field " + name)
	}
	return GraphNode{
		label:        label,
		fields:       []string{name},
		dependencies: dependencies,
		write:        make(chan DataFrameList),
		action:       MapNodeAction{name: name, m: MapType{nil: value}},
	}, nil
}

func (n MapNodeAction) Name() string {
	return "map"
}

// assumption: at least one DataFrameList exists
// assumption: does not modify keys
// assumption: joins exactly after target node
func (n MapNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {
	defer close(output)

	for {
		msg, more := <-input
		if !more {
			break
		}
		df := msg[len(msg)-1]
		for idx := range df.Rows {
			if v, ok := n.m[df.Rows[idx][n.name]]; ok {
				df.Rows[idx][n.name] = v
			}
		}
		output <- msg
	}
}

func (g *Graph) findFieldDependencies(name string) []GraphDependency {
	dependencies := []GraphDependency{}
	for _, node := range g.nodes {
		if Contains(node.fields, name) {
			dependencies = append(dependencies, GraphDependency{
				Node: node,
				Key:  []string{name},
			})

		}
	}
	return dependencies
}
