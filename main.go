package main

func main() {
	test1()
}

func test1() {

	backend := CreateDummyBackend()
	backend.AddCSVFile("graph", `product1,product2,score,category,segment
1,2,0.1,0,A
1,3,0.2,0,B
1,4,0.3,1,C
2,1,0.4,0,A
2,3,0.5,0,B
3,1,0.6,0,C`)
	backend.AddCSVFile("history", `user,product1,time
abc,1,1
abc,3,2
cde,2,3
fgh,4,9`)

	m := []map[string]interface{}{{"user": "abc"}, {"user": "cde"}}

	err := CreateGraphWithBackend(backend).
		Set(m, []string{"user"}).
		Query(QuerySpec{
			TableName: "history",
			Key:       []string{"user"},
			Fields:    []string{"product1", "time"},
		}).
		Query(QuerySpec{
			TableName: "graph",
			Key:       []string{"product1"},
			Fields:    []string{"product2", "score", "category", "segment"},
		}).
		Map("segment", MapType{"A": 1, "B": 2, "C": 3}).
		Filter("$segment >= 2").
		Order([]string{"-score"}).
		DiffFn("diff", "score", "diff", []string{"time"}, 42.0).
		Index("idx").
		Diff("diff2", "idx").
		FillNil("diff2", -1).
		// Order([]string{"-diff"}).
		Show().
		ToPrettyTable([]string{"idx", "time", "score", "category", "segment", "diff", "diff2"})
	if err != nil {
		panic(err)
	}
}

func test2() {
	backend := CreateDummyBackend()
	backend.AddCSVFile("graph", `product1,product2,score,category,segment
	1,2,0.1,0,A
	1,3,0.2,0,B
	1,4,0.3,1,C
	2,1,0.4,0,A
	2,3,0.5,0,B
	3,1,0.6,0,C`)
	backend.AddCSVFile("history", `user,product1,time
	abc,1,1
	abc,3,2
	cde,2,3
	fgh,4,9`)

	m := []map[string]interface{}{{"user": "fgh"}}

	err := CreateGraphWithBackend(backend).
		Set(m, []string{"user"}).
		Query(QuerySpec{
			TableName: "history",
			Key:       []string{"user"},
			Fields:    []string{"product1", "time"},
			Optional:  true,
		}).
		// Query(QuerySpec{
		// 	TableName: "graph",
		// 	Key:       []string{"product1"},
		// 	Fields:    []string{"product2", "score", "category"},
		// }).
		Show().
		ToPrettyTable([]string{"user", "product1", "product2", "score"})
	if err != nil {
		panic(err)
	}
}
