package db

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Schema struct {
	Name   string
	Tables map[string]Table
}

type Table struct {
	Name    string
	Columns map[string]Column
}

type ColumnKey string

const (
	ColumnKeyNone    ColumnKey = ""
	ColumnKeyPrimary ColumnKey = "PRI"
	ColumnKeyUnique  ColumnKey = "UNI"
	ColumnKeyIndex   ColumnKey = "MUL"
)

type Column struct {
	Name                string    `db:"Field"`
	Type                string    `db:"Type"`
	Null                string    `db:"Null"`
	Key                 ColumnKey `db:"Key"`
	Default             *string   `db:"Default",omitempty`
	Extra               string    `db:"Extra"`
	AutoIncrement       bool      `db:"-"`
	AutoIncrementOffset *int64    `db:"-"`
}

func (s *Schema) IsEqual(other Schema) error {
	if len(s.Tables) != len(other.Tables) {
		return errors.Errorf("table count mismatch: %d != %d", len(s.Tables), len(other.Tables))
	}

	for _, table := range s.Tables {
		otherTable := other.Tables[table.Name]
		err := validateTables(table, otherTable)
		if err != nil {
			return errors.Wrapf(err, "failed to compare tables: %s and %s", table.Name, otherTable.Name)
		}
	}

	return nil
}

func validateTables(table Table, otherTable Table) error {
	if table.Name != otherTable.Name {
		return errors.Errorf("table name mismatch: %s != %s", table.Name, otherTable.Name)
	}
	if len(table.Columns) != len(otherTable.Columns) {
		return errors.Errorf("column count mismatch: %d != %d", len(table.Columns), len(otherTable.Columns))
	}
	for _, column := range table.Columns {
		otherColumn, ok := otherTable.Columns[column.Name]
		if !ok {
			return errors.Errorf("column %s not found in other schema", column.Name)
		}

		if column.Name != otherColumn.Name {
			return errors.Errorf("column name mismatch: %s != %s", column.Name, otherColumn.Name)
		}
		if column.Type != otherColumn.Type {
			return errors.Errorf("column type mismatch: %s != %s", column.Type, otherColumn.Type)
		}
		if column.AutoIncrement != otherColumn.AutoIncrement {
			return errors.Errorf("column auto increment mismatch: %t != %t", column.AutoIncrement, otherColumn.AutoIncrement)
		}
	}
	return nil
}

func GetSchema(conn *sqlx.DB, databaseName string, ignoreTables []string) (Schema, error) {
	rows, err := conn.Queryx("SELECT table_name FROM information_schema.tables WHERE table_schema = ?", databaseName)
	if err != nil {
		return Schema{}, errors.Wrap(err, "failed to get tables")
	}
	defer rows.Close()

	var tableNames []string

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return Schema{}, errors.Wrap(err, "failed to scan table name")
		}

		if slices.Contains(ignoreTables, tableName) == false {
			tableNames = append(tableNames, tableName)
		}
	}

	tables := make(map[string]Table)
	mutex := sync.Mutex{}
	g := errgroup.Group{}

	for _, tableName := range tableNames {
		g.Go(func() error {
			table, err := newTable(conn, databaseName, tableName)
			if err != nil {
				return errors.Wrap(err, "failed to get table")
			}

			mutex.Lock()
			tables[table.Name] = table
			mutex.Unlock()

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return Schema{}, errors.Wrap(err, "failed to get tables")
	}

	return Schema{Name: databaseName, Tables: tables}, nil
}

func newTable(db *sqlx.DB, databaseName string, tableName string) (Table, error) {
	columns, err := getColumns(db, databaseName, tableName)
	if err != nil {
		return Table{}, errors.Wrap(err, "failed to get columns")
	}
	return Table{Name: tableName, Columns: columns}, nil
}

func getColumns(db *sqlx.DB, databaseName string, tableName string) (map[string]Column, error) {
	columns := make(map[string]Column)

	rows, err := db.Queryx(fmt.Sprintf("SHOW COLUMNS FROM %s.%s", databaseName, tableName))
	if err != nil {
		return columns, err
	}
	defer rows.Close()

	for rows.Next() {
		var column Column
		err = rows.StructScan(&column)
		if err != nil {
			return columns, errors.Wrap(err, "failed to scan column name and type")
		}

		if column.Extra == "auto_increment" {
			column.AutoIncrement = true

			offsetRows, err := db.Queryx(fmt.Sprintf("SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", databaseName, tableName))
			if err != nil {
				return columns, errors.Wrap(err, "failed to get auto increment offset")
			}
			defer offsetRows.Close()

			for offsetRows.Next() {
				var offset *int64
				err = offsetRows.Scan(&offset)
				if err != nil {
					return columns, errors.Wrap(err, "failed to scan auto increment offset")
				}
				column.AutoIncrementOffset = offset
			}
		}

		columns[column.Name] = column
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
