package db

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type Merger struct {
	newPrimaryKeysMap  map[string]map[string]map[int64]int64
	syncPrimaryKeysMap sync.Mutex

	Conn *sqlx.DB
}

func NewMerger(conn *sqlx.DB) *Merger {
	return &Merger{
		newPrimaryKeysMap: make(map[string]map[string]map[int64]int64),
		Conn:              conn,
	}
}

func (m *Merger) setPrimaryKey(tableName string, columnName string, oldValue int64, newValue int64) {
	m.syncPrimaryKeysMap.Lock()
	defer m.syncPrimaryKeysMap.Unlock()

	if _, ok := m.newPrimaryKeysMap[tableName]; !ok {
		m.newPrimaryKeysMap[tableName] = make(map[string]map[int64]int64)
	}
	if _, ok := m.newPrimaryKeysMap[tableName][columnName]; !ok {
		m.newPrimaryKeysMap[tableName][columnName] = make(map[int64]int64)
	}
	m.newPrimaryKeysMap[tableName][columnName][oldValue] = newValue
}

func (m *Merger) getPrimaryKey(tableName string, columnName string, value int64) (int64, error) {
	m.syncPrimaryKeysMap.Lock()
	defer m.syncPrimaryKeysMap.Unlock()

	if _, ok := m.newPrimaryKeysMap[tableName]; !ok {
		return 0, errors.Errorf("table %s not found", tableName)
	}
	if _, ok := m.newPrimaryKeysMap[tableName][columnName]; !ok {
		return 0, errors.Errorf("column %s not found", columnName)
	}
	if _, ok := m.newPrimaryKeysMap[tableName][columnName][value]; !ok {
		return 0, errors.Errorf("value %d not found", value)
	}

	return m.newPrimaryKeysMap[tableName][columnName][value], nil
}

func (m *Merger) MergeSchemas(source Schema, target Schema) error {
	err := source.IsEqual(target)
	if err != nil {
		return errors.Wrap(err, "schemas are not equal")
	}

	for _, table := range target.Tables {
		var allColumns []*Column
		var nonPrimaryKeyColumns []*Column
		var primaryKeyColumns []*Column
		for _, column := range table.Columns {
			allColumns = append(allColumns, &column)
			if column.Key == ColumnKeyPrimary && column.AutoIncrement && column.AutoIncrementOffset != nil {
				primaryKeyColumns = append(primaryKeyColumns, &column)
			} else {
				nonPrimaryKeyColumns = append(nonPrimaryKeyColumns, &column)
			}
		}

		if len(primaryKeyColumns) == 0 {
			//fmt.Printf("Skipping table %s, values: %+v\n", table.Name, table.Columns)
			continue
		}

		fmt.Printf(
			"INSERT INTO %s.%s (%s) SELECT %s, %s FROM %s.%s\n\n",
			target.Name,
			table.Name,
			strings.Join(lo.Map(allColumns, func(column *Column, _ int) string { return column.Name }), ", "),
			strings.Join(lo.Map(primaryKeyColumns, func(column *Column, _ int) string {
				return fmt.Sprintf("%s + %d", column.Name, *column.AutoIncrementOffset)
			}), ", "),
			strings.Join(lo.Map(nonPrimaryKeyColumns, func(column *Column, _ int) string { return column.Name }), ", "),
			source.Name,
			table.Name,
		)

		/*
			query := fmt.Sprintf(
				"SELECT %s FROM %s.%s",
				strings.Join(lo.Map(primaryKeyColumns, func(column *Column, _ int) string { return column.Name }), ", "),
				target.Name,
				table.Name,
			)

			fmt.Printf("%s\n", query)

			targetTablePKRows, err := m.Conn.Queryx(query)
			if err != nil {
				return errors.Wrap(err, "failed to get column rows")
			}

			for targetTablePKRows.Next() {
				for _, column := range primaryKeyColumns {
					var value int64
					err = targetTablePKRows.Scan(&value)
					if err != nil {
						return errors.Wrap(err, "failed to scan primary key value")
					}

					if column.AutoIncrementOffset == nil {
						panic("AutoIncrementOffset must be set")
					}

					m.setPrimaryKey(table.Name, column.Name, value, value+*column.AutoIncrementOffset)
				}
				if err != nil {
					return errors.Wrap(err, "failed to insert row")
				}

			}*/
	}

	return nil
}
