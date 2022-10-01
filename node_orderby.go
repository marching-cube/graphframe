package main

import (
	"sort"
	"strings"
)

type OrderByNodeAction struct {
	by []string
}

func (g *Graph) createOrderByNode(by []string, label string) (GraphNode, error) {
	return GraphNode{
		label:        label,
		fields:       nil,
		dependencies: []GraphDependency{{Node: g.nodes[len(g.nodes)-1]}},
		write:        make(chan DataFrameList),
		action:       OrderByNodeAction{by: by},
	}, nil
}

func (n OrderByNodeAction) Name() string {
	return "orderby"
}

func (n OrderByNodeAction) Execute(backend Backend, input chan DataFrameList, output chan DataFrameList) {
	defer close(output)

	for {
		dfs, more := <-input
		if !more {
			break
		}
		df := dfs[0]
		sort.Slice(df.Rows, func(i, j int) bool {
			for _, key := range n.by {
				if strings.HasPrefix(key, "-") {
					key = key[1:]
					i, j = j, i
				}
				vi := df.Rows[i][key]
				vj := df.Rows[j][key]
				if vi == vj {
					continue
				}
				switch vi_ := vi.(type) {
				case int:
					return vi_ < vj.(int)
				case int64:
					return vi_ < vj.(int64)
				case float64:
					return vi_ < vj.(float64)
				case string:
					return vi_ < vj.(string)
				default:
					panic("unsupported type?!")
				}
			}
			return true
		})

		output <- DataFrameList{df}
	}
}
