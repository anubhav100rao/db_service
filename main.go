package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type ConnectionPool struct {
	mu                 sync.Mutex
	pool               *sql.DB
	maxOpenConnections int
	numOpenConnections int
}

func NewConnectionPool(dataSourceName string, maxOpenConnections int) (*ConnectionPool, error) {
	pool, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(); err != nil {
		return nil, err
	}

	pool.SetMaxOpenConns(maxOpenConnections)
	pool.SetMaxIdleConns(maxOpenConnections / 2)

	return &ConnectionPool{
		pool:               pool,
		maxOpenConnections: maxOpenConnections,
		numOpenConnections: 0,
	}, nil
}
func (cp *ConnectionPool) GetConnection() (*sql.DB, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for {
		if cp.numOpenConnections < cp.maxOpenConnections {
			cp.numOpenConnections++
			return cp.pool, nil
		}

		if err := cp.pool.Ping(); err != nil {
			return nil, err
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (cp *ConnectionPool) ReleaseConnection(conn *sql.DB) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if conn == cp.pool {
		cp.numOpenConnections--
	} else {
		log.Fatal("trying to release a connection that doesn't belong to the pool")
	}
}

func main() {
	dataSourceName := "root@tcp(localhost:3306)/golanders" // Replace with your database details
	maxOpenConns := 10

	pool, err := NewConnectionPool(dataSourceName, maxOpenConns)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.pool.Close() // Close the pool when the program exits

	// Use the pool to acquire connections for database operations
	for i := 0; i < 5; i++ {
		conn, err := pool.GetConnection()
		if err != nil {
			log.Fatal(err)
		}

		// create a table in db
		_, err = conn.Exec(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS users%v (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))",
			i,
		))

		if err != nil {
			log.Fatal(err)
		}

		// Perform your database operations using conn

		defer func(conn *sql.DB) {
			pool.ReleaseConnection(conn)
		}(conn)
	}

	fmt.Println("Done")
}
