package main

type RootNodeAction struct {
	df DataFrame
}

func (g *Graph) createRootNode(df DataFrame, label string) (GraphNode, error) {
	return GraphNode{
		label:        label,
		fields:       df.Header,
		dependencies: nil,
		write:        make(chan DataFrameList),
		action:       RootNodeAction{df: df},
	}, nil
}

func (n RootNodeAction) Name() string {
	return "root"
}

func (n RootNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {
	defer close(output)
	output <- []DataFrame{n.df}
}
