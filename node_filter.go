package main

type FilterNodeAction struct {
	f FilterSpec
}

func (g *Graph) createFilterNode(text string, label string) (GraphNode, error) {
	f, err := createFilterSpec(text)
	if err != nil {
		return GraphNode{}, err
	}
	fields := f.Variables()[0] // TODO: more than one!

	return GraphNode{
		label:        label,
		fields:       nil,  // TODO: correct ?
		dependencies: g.findFieldDependencies(fields),
		write:        make(chan DataFrameList),
		action:       FilterNodeAction{f: f},
	}, nil
}

func (n FilterNodeAction) Name() string {
	return "filter"
}

func (n FilterNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {
	defer close(output)
	for {
		dfs, more := <- input
		if !more {
			break
		}

		df := dfs[len(dfs)-1]
		ndf := n.f.Filter(df)
		output <- append(dfs[:len(dfs)-1], ndf)
		
	}
}
