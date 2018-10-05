// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
)

func requireArg(arg string, name string) {
	if arg == "" {
		log.Fatalf("Missing required argument: -%s\n", name)
	}
}

func main() {
	var (
		project, instance, table string
		family, column           string
		n                        int
	)

	flag.StringVar(&project, "project", "", "project ID")
	flag.StringVar(&instance, "instance", "", "instance ID")
	flag.StringVar(&table, "table", "", "table")
	flag.StringVar(&family, "family", "", "column family")
	flag.StringVar(&column, "column", "", "column")
	flag.Parse()

	requireArg(project, "project")
	requireArg(instance, "instance")
	requireArg(table, "table")
	requireArg(family, "family")
	requireArg(column, "column")

	ctx := context.Background()

	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		log.Fatal(err)
	}
	tbl := client.Open(table)

	buf := NewBuffer(family, column, 10000)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		a := strings.Split(line, "\t")
		if len(a) != 2 {
			log.Fatalf("invalid input line: '%s'", line)
		}
		if a[0] == "" {
			log.Fatalf("invalid row key: '%s'", a[0])
		}
		buf.Add(a[0], a[1])
		n += buf.Flush(ctx, tbl)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	n += buf.Flush(ctx, tbl)
	fmt.Printf("wrote %d rows\n", n)
}

// MutationBuffer buffers mutations
type MutationBuffer struct {
	family    string
	column    string
	rowKeys   []string
	mutations []*bigtable.Mutation
	i         int
	n         int
}

// NewBuffer initializes a new buffer
func NewBuffer(family, column string, n int) *MutationBuffer {
	return &MutationBuffer{
		family:    family,
		column:    column,
		rowKeys:   make([]string, 0, n),
		mutations: make([]*bigtable.Mutation, 0, n),
		i:         -1,
		n:         n,
	}
}

// Add values for a given key
func (buf *MutationBuffer) Add(k string, v string) {
	mutation := bigtable.NewMutation()
	mutation.Set(buf.family, buf.column, bigtable.ServerTime, []byte(v))
	buf.rowKeys = append(buf.rowKeys, k)
	buf.mutations = append(buf.mutations, mutation)
	buf.i++
}

// Clear empties the buffer
func (buf *MutationBuffer) Clear() {
	buf.rowKeys = buf.rowKeys[:0]
	buf.mutations = buf.mutations[:0]
	buf.i = -1
}

// Remaining returns number of values in buffer
func (buf *MutationBuffer) Remaining() int {
	return buf.n - (buf.i + 1)
}

// Flush sends bulk mutation request to BigTable
func (buf *MutationBuffer) Flush(ctx context.Context, table *bigtable.Table) int {
	n := buf.i + 1
	if n == 0 {
		return 0
	}
	t0 := time.Now()

	errs, err := table.ApplyBulk(ctx, buf.rowKeys, buf.mutations)
	if err != nil {
		log.Printf("%d errors: %s", len(errs), err.Error())
	}
	t1 := time.Now()
	dt := (t1.UnixNano() - t0.UnixNano()) / 1e6
	buf.Clear()
	log.Printf("wrote %d rows in %d ms", n, dt)
	return n
}
