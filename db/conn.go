package db

import (
	"fmt"
	"net/url"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// convert from user:password@localhost:5555/dbname user:password@tcp(localhost:5555)/dbname
func dbUrlToDsn(dbUrl string) (string, error) {
	u, err := url.Parse(dbUrl)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse db url")
	}

	// Extract user info (username and password)
	userInfo := u.User
	username := userInfo.Username()
	password, _ := userInfo.Password()

	// Extract host and port
	host := u.Hostname()
	port := u.Port()

	// Format the DSN string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", username, password, host, port)

	return dsn, nil
}

func NewConnection(dbUrl string) (*sqlx.DB, error) {
	dsn, err := dbUrlToDsn(dbUrl)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert db url to dsn")
	}

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open db")
	}

	db.SetMaxOpenConns(100)  // The default is 0 (unlimited)
	db.SetMaxIdleConns(2)    // defaultMaxIdleConns = 2
	db.SetConnMaxLifetime(0) // 0, connections are reused forever.

	return db, nil
}
