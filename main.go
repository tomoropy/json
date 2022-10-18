package main

import (
	"bufio"
	"database/sql"
	"encoding/json"

	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type Log struct {
	User User `json:"user"`
}

type Logs []Log

type User struct {
	Age  int    `json:"age"`
	Name string `json:"name"`
	Role string `json:"role"`
}

func main() {
	// conection to postgres
	db, err := sql.Open("postgres", "host=127.0.0.1 port=5433 user=postgres password=postgres sslmode=disable")
	defer db.Close()

	if err != nil {
		fmt.Println(err)
	}

	// create table
	if _, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS user_table (
		id serial PRIMARY KEY, 
		age INTEGER, 
		name VARCHAR(500), 
		role CHAR(15)
		);`); err != nil {
		fmt.Println(err)
	}

	fullPath := "./" + os.Args[1]
	if len(os.Args) > 2 {
		fmt.Println("引数に渡すファイルは一つだけにしてください")
		os.Exit(1)
	}

	fp, err := os.Open(fullPath)
	if err != nil {
		fmt.Println(err)
	}

	defer fp.Close()

	scanner := bufio.NewScanner(fp)

	tx, err := db.Begin()

	for scanner.Scan() {
		var log Log

		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			fmt.Println(err)
		}

		cmd := `INSERT INTO user_table (
			age,
			name,
			role) VALUES ($1,$2,$3);`

		_, err = tx.Exec(cmd,
			log.User.Age,
			log.User.Name,
			log.User.Role,
		)

		// when errors occur somewhere
		if err != nil {
			fmt.Println(err)
			tx.Rollback() // rollback
			os.Exit(1)    // exit code
		}
	}
	// when error doesn't occur
	tx.Commit()
}
