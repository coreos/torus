package main

import (
	"encoding/csv"
	"io"
	"strings"
	"text/tabwriter"
)

type TableWriter struct {
	output io.Writer
	rows   [][]string
	header []string
}

func NewTableWriter(output io.Writer) *TableWriter {
	return &TableWriter{
		output: output,
	}
}

func (t *TableWriter) Render() {
	tw := tabwriter.NewWriter(t.output, 5, 1, 2, ' ', 0)
	capsHeader := make([]string, len(t.header))
	for i, x := range t.header {
		capsHeader[i] = strings.ToUpper(x)
	}
	tw.Write([]byte(strings.Join(capsHeader, "\t") + "\n"))
	for _, x := range t.rows {
		tw.Write([]byte(strings.Join(x, "\t") + "\n"))
	}
	tw.Flush()
}

func (t *TableWriter) RenderCSV() {
	cw := csv.NewWriter(t.output)
	cw.WriteAll(t.rows)
	cw.Flush()
}

func (t *TableWriter) Append(s []string) {
	t.rows = append(t.rows, s)
}

func (t *TableWriter) SetHeader(s []string) {
	t.header = s
}

// TODO(barakmich) TableWriter.SortBy?
