package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"runtime"
	"strings"
	"time"

	"github.com/hashicorp/hcl"
	"github.com/jackc/pgx"
)

type Config struct {
	// Database connection information
	Database string
	Host     string
	Port     int
	User     string
	Password string

	Op     string
	Table  string
	Column string
	Vary   string

	// Fields below can "Vary"

	// Duration of run
	Dur time.Duration

	// How many concurrent workers to use.
	Workers int

	// Rate of attack
	Rate       int // Total
	WorkerRate int // Per-worker

	Configs []Config
}

func (c Config) String() string {
	return fmt.Sprintf("db %#v %s@%s:%d %s %s x %d rate [%d/s %d/s]",
		c.Database, c.User, c.Host, c.Port, c.Op, c.Dur,
		c.Workers, c.Rate, c.WorkerRate)
}

func (a Config) Merge(b Config) Config {
	if a.Database == "" {
		a.Database = b.Database
	}
	if a.Host == "" {
		a.Host = b.Host
	}
	if a.Port == 0 {
		a.Port = b.Port
	}
	if a.User == "" {
		a.User = b.User
	}
	if a.Password == "" {
		a.Password = b.Password
	}

	if a.Op == "" {
		a.Op = b.Op
	}
	if a.Table == "" {
		a.Table = b.Table
	}
	if a.Column == "" {
		a.Column = b.Column
	}
	if a.Vary == "" {
		a.Vary = b.Vary
	}
	if a.Dur == 0 {
		a.Dur = b.Dur
	}
	if a.Workers == 0 {
		a.Workers = b.Workers
	}
	if a.Rate == 0 {
		a.Rate = b.Rate
	}
	if a.WorkerRate == 0 {
		a.WorkerRate = b.WorkerRate
	}
	return a
}

func (config Config) ConnConfig() pgx.ConnConfig {
	host := config.Host
	hosts := strings.Split(host, ",")
	if len(hosts) > 0 {
		host = hosts[rand.Intn(len(hosts))]
	}

	return pgx.ConnConfig{
		Database: config.Database,
		Host:     host,
		Port:     uint16(config.Port),
		User:     config.User,
		Password: config.Password,
	}
}

var DefaultConfig = Config{
	Database: "pgslam",
	Host:     "localhost",
	User:     "pgslam",
	Password: "pgslam",
	Workers:  runtime.NumCPU(),

	Rate: 100,
	Dur:  time.Second * 10,
}

func getconfigs() ([]*Config, error) {
	configs := []*Config{}
	config := Config{}
	flag.StringVar(&config.Database, "db", DefaultConfig.Database, "Database")
	flag.StringVar(&config.Host, "host", DefaultConfig.Host, "Host")
	flag.IntVar(&config.Port, "port", DefaultConfig.Port, "Port")
	flag.StringVar(&config.User, "user", DefaultConfig.User, "User")
	flag.StringVar(&config.Password, "pass", DefaultConfig.Password, "Password")

	flag.StringVar(&config.Op, "op", "", "Operation")
	flag.StringVar(&config.Table, "table", "", "Table")
	flag.StringVar(&config.Column, "column", "", "Column")
	flag.DurationVar(&config.Dur, "dur", DefaultConfig.Dur, "Duration")

	flag.IntVar(&config.Workers, "workers", DefaultConfig.Workers, "Workers")

	flag.IntVar(&config.Rate, "rate", DefaultConfig.Rate, "Operation rate per second")
	flag.IntVar(&config.WorkerRate, "workerrate", DefaultConfig.WorkerRate, "Operation rate per second per worker")
	flag.Parse()

	files := flag.Args()
	if len(files) == 0 {
		configs = append(configs, &config)
	}
	for _, file := range files {
		c, err := loadconfig(file)
		if err != nil {
			return nil, err
		}
		//fmt.Println("loaded", &c)
		more := c.Configs
		c.Configs = nil

		c = c.Merge(DefaultConfig)
		c = config.Merge(c)

		if len(more) == 0 {
			configs = append(configs, &c)
			//fmt.Println("append", &c)
		}
		for _, m := range more {
			mm := m.Merge(c)
			configs = append(configs, &mm)
			//fmt.Println("append", &mm)
		}
	}
	return configs, nil
}

func loadconfig(path string) (Config, error) {
	r := Config{}
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	if err := hcl.Unmarshal(buf, &r); err != nil {
		return Config{}, err
	}

	return r, nil
}
