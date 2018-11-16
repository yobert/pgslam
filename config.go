package main

import (
	"io/ioutil"
	"runtime"
	"time"

	"github.com/hashicorp/hcl"
	"github.com/jackc/pgx"
)

type Config struct {
	// Database connection information
	Database string
	Host     string
	Port     uint16
	User     string
	Password string

	Op    string
	Table string
	Vary  string

	// Fields below can "Vary"

	// Duration of run
	Dur time.Duration

	// How many concurrent workers to use.
	Workers int

	// Rate of attack
	Rate       int // Total
	WorkerRate int // Per-worker
}

func (config Config) ConnConfig() pgx.ConnConfig {
	return pgx.ConnConfig{
		Database: config.Database,
		Host:     config.Host,
		Port:     config.Port,
		User:     config.User,
		Password: config.Password,
	}
}

var DefaultConfig = Config{
	Database: "pgslam",
	Host:     "localhost",

	Port:     5432,
	User:     "pgslam",
	Password: "pgslam",

	/*	Port: 26257,
		User: "root",*/

	Dur:     time.Second * 10,
	Workers: 50,
	Rate:    1000,
}

func loadconfig(path string) (*Config, error) {
	_ = runtime.NumCPU
	config := DefaultConfig

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := hcl.Unmarshal(buf, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
