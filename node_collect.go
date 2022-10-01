package main

type CollectNodeAction struct {
}

func (g *Graph) createCollectNode(label string) (GraphNode, error) {
	return GraphNode{
		label:        label,
		fields:       []string{},
		dependencies: g.findLeafDependencies(),
		write:        make(chan DataFrameList),
		action:       CollectNodeAction{},
	}, nil
}

func (n CollectNodeAction) Name() string {
	return "collect"
}

func (n CollectNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {

	defer close(output)

	var df DataFrame

	for {
		row, more := <-input
		if !more {
			break
		}
		df = df.Concat(row.Flatten())
	}

	output <- DataFrameList{df}
}

func (g *Graph) findLeafDependencies() []GraphDependency {
	m := map[string]bool{}
	for _, node := range g.nodes {
		for _, previous := range node.dependencies {
			m[previous.Node.label] = true
		}
	}
	leaves := []GraphNode{}
	for _, node := range g.nodes {
		if !m[node.label] {
			leaves = append(leaves, node)
		}
	}

	dependencies := []GraphDependency{}
	for _, leaf := range leaves {
		dependencies = append(dependencies, GraphDependency{Node: leaf})
	}

	return dependencies
}
