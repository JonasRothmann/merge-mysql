package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type Schema struct {
	Tables []Table
}

type Table struct {
	Name    string
	Columns []Column
}

type Column struct {
	Name string
	Type string
}

func (s *Schema) IsEqual(other Schema) bool {
	if len(s.Tables) != len(other.Tables) {
		return false
	}

	for i, table := range s.Tables {
		if table.Name != other.Tables[i].Name {
			return false
		}
		if len(table.Columns) != len(other.Tables[i].Columns) {
			return false
		}
		for j, column := range table.Columns {
			if column.Name != other.Tables[i].Columns[j].Name {
				return false
			}
			if column.Type != other.Tables[i].Columns[j].Type {
				return false
			}
		}
	}

	return true
}

func getSchema(dbUrl string) (Schema, error) {
	db, err := sql.Open("mysql", dbUrl)
	if err != nil {
		return Schema{}, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return Schema{}, err
	}
	defer rows.Close()

	var tables []Table

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return Schema{}, err
		}

		columns, err := getColumns(db, tableName)
		if err != nil {
			return Schema{}, err
		}

		tables = append(tables, Table{Name: tableName, Columns: columns})
	}

	return Schema{Tables: tables}, nil
}

func getColumns(db *sql.DB, tableName string) ([]Column, error) {
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", tableName))
	if err != nil {
		return []Column{}, err
	}
	defer rows.Close()

	var columns []Column

	for rows.Next() {
		var columnName string
		var columnType string
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return []Column{}, err
		}

		columns = append(columns, Column{Name: columnName, Type: columnType})
	}

	return columns, nil
}

func (s *Schema) String() string {
	var tableStrings []string
	for _, table := range s.Tables {
		tableStrings = append(tableStrings, table.String())
	}
	return strings.Join(tableStrings, "\n")
}

func (t *Table) String() string {
	var columnStrings []string
	for _, column := range t.Columns {
		columnStrings = append(columnStrings, column.String())
	}
	return fmt.Sprintf("Table %s\n%s", t.Name, strings.Join(columnStrings, "\n"))
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Type)
}
