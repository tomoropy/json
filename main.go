package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

type Data struct {
	User User `json:"user"`
}

type DataLog []Data

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
		log.Fatalln(err)
	}

	// create table
	if _, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS user_table (
		id serial PRIMARY KEY, 
		age INTEGER, 
		name VARCHAR(500), 
		role CHAR(15)
		);`); err != nil {
		log.Fatalln(err)
	}

	if len(os.Args) > 2 {
		fmt.Println("引数に渡すファイルは一つにしてください")
		os.Exit(1)
	}

	fp, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}

	defer fp.Close()

	scanner := bufio.NewScanner(fp)

	tx, err := db.Begin()
	if err != nil {
		log.Fatalln(err)
	}

	cmd := `INSERT INTO user_table (
		age,
		name,
		role) VALUES ($1,$2,$3);`

	for scanner.Scan() {
		var data Data

		if err := json.Unmarshal(scanner.Bytes(), &data); err != nil {
			log.Fatalln(err)
			continue
		}

		_, err = tx.Exec(cmd,
			data.User.Age,
			data.User.Name,
			data.User.Role,
		)

		// when errors occur somewhere
		if err != nil {
			if err = tx.Rollback(); err != nil {
				fmt.Println(err)
			}
			os.Exit(1)
		}
	}
	// when error doesn't occur
	if err = tx.Commit(); err != nil {
		log.Fatalln(err)
	}
}
