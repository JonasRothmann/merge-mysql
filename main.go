package main

import (
	"fmt"
	"os"

	"github.com/JonasRothmann/merge-mysql/db"
)

func main() {
	args := os.Args[1:]

	if len(args) < 2 {
		println("Usage: merge-mysql <source-db-url> <target-db-url> [...ignored-tables]\nUrl format: mysql://user:password@host:port/database")
		os.Exit(1)
	}

	url := args[0]
	sourceDatabase := args[1]
	targetDatabase := args[2]

	ignoredTables := make([]string, 0)

	for i := 2; i < len(args); i++ {
		ignoredTables = append(ignoredTables, args[i])
	}

	conn, err := db.NewConnection(url)
	if err != nil {
		fmt.Printf("Error: Could not connect to source database: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	sourceSchema, err := db.GetSchema(conn, sourceDatabase, ignoredTables)
	if err != nil {
		fmt.Printf("Error: Could not get schema from source database: %s\n", err)
		os.Exit(1)
	}
	targetSchema, err := db.GetSchema(conn, targetDatabase, ignoredTables)
	if err != nil {
		fmt.Printf("Error: Could not get schema from target database: %s\n", err)
		os.Exit(1)
	}

	merger := db.NewMerger(conn)
	err = merger.MergeSchemas(sourceSchema, targetSchema)
	if err != nil {
		fmt.Printf("Error: Could not merge schemas: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully merged schemas from %s and %s\n", sourceSchema.Name, targetSchema.Name)
}
