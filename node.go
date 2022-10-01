package main

import (
	"sync"
)

type GraphAction interface {
	Execute(backend Backend, input chan DataFrameList, output chan DataFrameList)
	Name() string
}

type GraphNode struct {
	label        string
	fields       []string
	dependencies []GraphDependency
	write        chan DataFrameList
	action       GraphAction
}

func (n GraphNode) Execute(backend Backend) {

	if len(n.dependencies) == 0 {
		n.action.Execute(backend, nil, n.write)
		return
	}
	if len(n.dependencies) == 1 {
		n.action.Execute(backend, n.dependencies[0].Node.write, n.write)
		return
	}

	var wg sync.WaitGroup

	agg := make(chan DataFrameList)
	go func() {
		for _, dep := range n.dependencies {
			wg.Add(1)
			go func(c chan DataFrameList) {
				defer wg.Done()
				for {
					msg, more := <-c
					if !more {
						return
					}
					agg <- msg
				}
			}(dep.Node.write)
		}
		wg.Wait()
		close(agg)
	}()

	n.action.Execute(backend, agg, n.write)
}

func (n GraphNode) TypeName() string {
	return n.action.Name()
}
