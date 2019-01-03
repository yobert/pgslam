package main

import (
	"fmt"

	"github.com/jackc/pgx"
)

func prepSchema(config Config) error {

	if err := createDatabase(config); err != nil {
		return err
	}

	sql := config.Prep
	if len(sql) == 0 {
		return nil
	}

	connconfig := config.ConnConfig(0)
	conn, err := pgx.Connect(connconfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Println(debugsql(sql, nil))
	if _, err := conn.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createDatabase(config Config) error {
	connconfig := config.ConnConfig(0)
	connconfig.Database = "postgres"

	conn, err := pgx.Connect(connconfig)
	if err != nil {
		return err
	}
	defer conn.Close()

	sql := `create database ` + config.Database

	fmt.Println(debugsql(sql, nil))
	if _, err := conn.Exec(sql); err != nil {
		return err
	}

	return nil
}
