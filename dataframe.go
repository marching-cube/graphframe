package main

import (
	"strings"
)

type DataFrame struct {
	Index    []string
	Header   []string
	Rows     []map[string]interface{}
	Optional bool
}

func ReadCSV(text string) DataFrame {
	rows := []map[string]interface{}{}
	lines := strings.Split(text, "\n")
	header := strings.Split(lines[0], ",")
	for _, row := range lines[1:] {
		sml := map[string]interface{}{}
		for idx, v := range strings.Split(row, ",") {
			sml[header[idx]] = v
		}
		rows = append(rows, sml)
	}
	return DataFrame{Header: header, Rows: rows}
}

func (df DataFrame) IsEmpty() bool {
	return len(df.Rows) == 0 || len(df.Header) == 0
}

func (df DataFrame) Query(keys []string, values [][]string) DataFrame {

	rows := []map[string]interface{}{}
	for _, row := range df.Rows {
		hit := false
		for _, key := range values {
			match := true
			for idx, v := range key {
				if v_, ok := row[keys[idx]]; ok {
					match = match && v_ == v
				} else {
					match = false
				}
			}
			hit = hit || match
		}
		if hit {
			rows = append(rows, row)
		}
	}

	return DataFrame{Header: df.Header, Rows: rows, Index: keys}
}

func (df DataFrame) GroupBy(by []string) map[string][]map[string]interface{} {
	r := map[string][]map[string]interface{}{}
	for _, row := range df.Rows {
		pk, _ := CreateKeyCode(row, by)
		if _, ok := r[pk]; !ok {
			r[pk] = []map[string]interface{}{row}
		} else {
			r[pk] = append(r[pk], row)
		}
	}
	return r
}

func (df DataFrame) Concat(df2 DataFrame) DataFrame {
	if df.IsEmpty() {
		return df2
	}
	if df2.IsEmpty() {
		return df
	}
	// TODO: same header!
	return DataFrame{
		Index:  df.Index,
		Header: df.Header,
		Rows:   append(df.Rows, df2.Rows...),
	}
}

type DataFrameList []DataFrame

func (dfs DataFrameList) Flatten() DataFrame {

	if len(dfs) == 0 {
		return DataFrame{}
	}
	if len(dfs) == 1 {
		return dfs[0]
	}

	// root
	headers := append([]string{}, dfs[0].Header...)
	rows := []map[string]interface{}{}
	for _, seed := range dfs[0].Rows {
		row := map[string]interface{}{}
		for k, v := range seed {
			row[k] = v
		}
		rows = append(rows, row)
	}

	// the rest
	for _, data := range dfs[1:] {
		tmp := []map[string]interface{}{}
		m := data.GroupBy(data.Index)
		for _, seed := range rows {
			code, _ := CreateKeyCode(seed, data.Index)
			if group, ok := m[code]; ok {
				for _, row := range group {
					tmp = append(tmp, MapConcat(seed, row))
				}
			} else if data.Optional {
				tmp = append(tmp, seed)
			}
		}
		rows = tmp
		for _, h := range data.Header {
			if !Contains(headers, h) {
				headers = append(headers, h)
			}
		}
	}

	return DataFrame{
		Header: headers,
		Rows:   rows,
	}
}
