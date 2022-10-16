package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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

	file, err := ioutil.ReadFile("./sample.log")

	if err != nil {
		log.Println("ReadError: ", err)
		os.Exit(1)
	}

	var logs Logs

	if err := json.Unmarshal(file, &logs); err != nil {
		log.Fatalln(err)
	}

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

	// insert data
	// transction process
	tx, _ := db.Begin()
	for _, p := range logs {

		cmd := `INSERT INTO user_table (
			age,
			name,
			role) VALUES ($1,$2,$3);`

		_, err = tx.Exec(cmd,
			p.User.Age,
			p.User.Name,
			p.User.Role,
		)

		if err != nil {
			fmt.Println(err)
		}
	}
	// when errors occur
	if err != nil {
		tx.Rollback()
	} else {
		tx.Commit()
	}
}
