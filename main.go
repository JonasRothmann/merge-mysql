package main

import "os"

func main() {
	args := os.Args[1:]

	if len(args) != 2 {
		println("Usage: merge-mysql <source-db-url> <target-db-url>\nUrl format: mysql://user:password@host:port/database")
		os.Exit(1)
	}

	sourceURL := args[0]
	targetURL := args[1]

	sourceSchema, err := getSchema(source)
	if err != nil {
		println("Error: Could not get schema from source database")
		os.Exit(1)
	}
	targetSchema, err := getSchema(target)
	if err != nil {
		println("Error: Could not get schema from target database")
		os.Exit(1)
	}

	if sourceSchema.IsEqual(targetSchema) {
		println("Schemas are equal")
		os.Exit(0)
	} else {
		println("Schemas are not equal")
	}

	println("Should merge tables")
}
