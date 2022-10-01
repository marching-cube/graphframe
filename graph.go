package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
)

type Graph struct {
	backend  Backend
	fields   []string // TODO: used ???
	nodes    []GraphNode
	selected []GraphNode
	err      error // TODO: use this

	exectued  int  // TODO: implement this
	collected bool // TODO: implement this

}

type GraphDependency struct {
	Node GraphNode
	Key  []string // TODO: used?
}

type QuerySpec struct {
	TableName string
	Key       []string
	Fields    []string
	Query     string
	Limit     int
	Optional  bool
}

func CreateGraphWithBackend(backend Backend) *Graph {
	return &Graph{
		backend: backend,
	}
}

// TODO: append????
// TODO: what about fields?
// TODO: rename
func (g *Graph) Set(rows []map[string]interface{}, fields []string) *Graph {
	df := DataFrame{Header: fields, Rows: rows}
	node, err := g.createRootNode(df, g.autoLabel())
	if err != nil {
		g.err = err
		return g
	}
	g.appendNode(node)
	return g
}

func (g *Graph) autoLabel() string {
	return "#" + strconv.Itoa(len(g.nodes))
}

func (g *Graph) appendNode(n GraphNode) {
	g.nodes = append(g.nodes, n)
	g.selected = append(g.selected, n)
	for _, field := range n.fields {
		if !Contains(g.fields, field) {
			g.fields = append(g.fields, field)
		}
	}
}

func (g *Graph) Query(query QuerySpec) *Graph {
	node, err := g.createQueryNode(query, g.autoLabel())
	if err != nil {
		g.err = err
	}

	g.appendNode(node)
	return g
}

func (g *Graph) Filter(text string) *Graph {
	node, err := g.createFilterNode(text, g.autoLabel())
	if err != nil {
		g.err = err
	}
	
	g.appendNode(node)
	return g
}

func (g *Graph) SelectNodesWithLabels(labels []string) *Graph {
	selected := []GraphNode{}
	for _, node := range g.nodes {
		if Contains(labels, node.label) {
			selected = append(selected, node)
		}
	}
	g.selected = selected
	return g
}

func (g *Graph) SelectAllNodes() *Graph {
	g.selected = g.nodes
	return g
}

func (g *Graph) Collect() *Graph {
	if g.collected || g.err != nil || len(g.nodes) == 0 {
		return g
	}
	g.collected = true
	node, err := g.createCollectNode(g.autoLabel())
	if err != nil {
		g.err = err
		return g
	}
	g.appendNode(node)
	return g
}

func (g *Graph) Fetch() *Graph {
	g.Collect()
	if g.exectued == len(g.nodes) || g.err != nil || len(g.nodes) == 0 {
		return g
	}
	g.Execute()
	return g
}

func (g *Graph) result() (chan DataFrameList, error) {
	g.Fetch()
	if g.err != nil {
		return nil, g.err
	}
	return g.nodes[len(g.nodes)-1].write, nil
}

func (g *Graph) Order(by []string) *Graph {
	g.Collect()
	node, _ := g.createOrderByNode(by, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) Map(name string, m MapType) *Graph {
	node, _ := g.createMapNode(name, m, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) FillNil(name string, value any) *Graph {
	node, _ := g.createFillNilNode(name, value, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) Diff(name string, source string) *Graph {
	g.Fetch()
	node, _ := g.createDiffNode(name, source, "diff", nil, nil, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) Lag(name string, source string) *Graph {
	g.Fetch()
	node, _ := g.createDiffNode(name, source, "lag", nil, nil, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) Lead(name string, source string) *Graph {
	g.Fetch()
	node, _ := g.createDiffNode(name, source, "lead", nil, nil, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) Index(name string) *Graph {
	g.Fetch()
	node, _ := g.createDiffNode(name, g.fields[0], "index", nil, nil, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) Rank(name string, source string) *Graph {
	g.Fetch()
	node, _ := g.createDiffNode(name, source, "rank", nil, nil, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) DiffFn(name string, source string, fn string, partitionby []string, default_ interface{}) *Graph {
	g.Fetch()
	node, _ := g.createDiffNode(name, source, fn, partitionby, default_, g.autoLabel())
	g.appendNode(node)
	return g
}

func (g *Graph) ToDataFrame() (DataFrame, error) {
	result, err := g.result()
	if err != nil {
		return DataFrame{}, g.err
	}
	dfs := <-result

	return dfs[0], nil
}

func (g *Graph) ToFloatArray(fields []string) ([][]float64, error) {

	rows, err := g.ToDataFrame()
	if err != nil {
		return nil, err
	}

	result := make([][]float64, len(rows.Rows), len(rows.Rows))
	for jdx, row := range rows.Rows {
		line := make([]float64, len(fields), len(fields))
		for idx, name := range fields {
			v, err := ToFloat64(row[name])
			if err != nil {
				return result, err
			}
			line[idx] = v
		}
		result[jdx] = line
	}
	return result, nil
}

func (g *Graph) ToCSV(fields []string) (string, error) {
	rows, err := g.ToDataFrame()
	if err != nil {
		return "", err
	}

	result := strings.Join(fields, ",") + "\n"
	for _, row := range rows.Rows {
		line := make([]string, len(fields), len(fields))
		for idx, name := range fields {
			v, err := ToString(row[name])
			if err != nil {
				return result, err
			}
			line[idx] = v
		}
		result += strings.Join(line, ",") + "\n"
	}
	return result, nil
}

func (g *Graph) ToPrettyTable(fields []string) error {
	rows, err := g.ToDataFrame()
	if err != nil {
		return err
	}
	header := table.Row{}
	for _, field := range fields {
		header = append(header, field)
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(header)
	for _, row := range rows.Rows {
		line := table.Row{}
		for _, name := range fields {
			line = append(line, row[name])
		}
		t.AppendRow(line)

	}
	t.Render()
	return nil
}

func (g *Graph) Show() *Graph {

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"label", "type", "fields", "after"})
	for _, n := range g.nodes {
		labels := []string{}
		for _, dep := range n.dependencies {
			labels = append(labels, dep.Node.label)
		}
		t.AppendRow(table.Row{n.label, n.TypeName(), n.fields, labels})
	}
	t.Render()
	return g
}

// TODO: sync or async?
func (g *Graph) Execute() *Graph {
	for idx := g.exectued; idx < len(g.nodes); idx++ {
		node := g.nodes[idx]
		go node.Execute(g.backend)
	}
	g.exectued = len(g.nodes)
	return g
}
