package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
)

type (
	Connection struct {
		db               *sql.DB
		connectionString *string
		User             string  `json:"user,omitempty"`
		Password         string  `json:"password,omitempty"`
		Method           string  `json:"method,omitempty"`
		Host             string  `json:"host,omitempty"`
		Port             string  `json:"port,omitempty"`
		Schema           string  `json:"schema,omitempty"`
		Query            *string `json:"query,omitempty"`
		sync.RWMutex
	}
)

// Db is the method that should be generally called, (E.g. variable.Db().prepare("INSERT INTO...")
func (c *Connection) Db() *sql.DB {
	if c.db == nil {
		c.connect()
	}

	c.RLock()
	defer c.RUnlock()

	return c.db
}

// SetDb should be avoided generally and is there for overriding purposes only
func (c *Connection) SetDb(db *sql.DB) {
	c.Lock()
	defer c.Unlock()
	c.db = db
}

func (c *Connection) Ping() {
	if c.db == nil {
		c.connect()
	}

	c.RLock()
	defer c.RUnlock()

	if err := c.db.Ping(); err != nil {
		c.db = nil
		panic(err)
	}
}

func (c *Connection) connect() {
	defer c.Unlock()
	c.Lock()

	if c.connectionString == nil {
		c.generateConnectionString()
	}

	db, err := sql.Open("mysql", *c.connectionString)
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		if err := db.Close(); err != nil {
			log.Println("closing db error:", err)
		}

		panic(err)
	}

	c.db = db
}

func (c *Connection) generateConnectionString() {
	if c.User == "" || c.Password == "" || c.Method == "" && c.Host == "" ||
		c.Port == "" && c.Schema == "" {
		panic("invalid mysql")
	}

	// user:pssword@method(destination:port)/schema?query
	// Example: root:password@tcp(127.0.0.1:3306)/schema?timeout=2s&parseTime=true
	connectionString := fmt.Sprintf("%s:%s@%s(%s:%s)/%s", c.User, c.Password, c.Method, c.Host, c.Port, c.Schema)

	if c.Query != nil {
		connectionString = fmt.Sprintf("%s?%s", connectionString, *c.Query)
	}

	c.connectionString = &connectionString
}
