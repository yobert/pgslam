package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/hcl"
	"github.com/jackc/pgx"
)

type Config struct {
	Configs []Config

	// Database connection information
	Database string
	Host     string
	User     string
	Password string

	Nodes int

	Prep   string
	Exec   string
	Values []ConfigValue

	Vary string

	// Duration of run
	Dur time.Duration

	// How many concurrent workers to use.
	Workers int

	// Rate of attack
	Rate int // Total
}
type ConfigValue struct {
	Type   string
	String string
}

var DefaultConfig = Config{
	Database: "pgslam",
	Host:     "localhost:26257",
	User:     "pgslam",
	Password: "pgslam",
	Workers:  runtime.NumCPU(),

	Rate: 100,
	Dur:  time.Second * 10,
}

func (c Config) String() string {
	return fmt.Sprintf("db %#v %s@%s %s x %d [%d/s]",
		c.Database, c.User, c.Host, c.Dur,
		c.Workers, c.Rate)
}

func (c Config) TitleParts() []string {
	return []string{
		fmt.Sprintf("%d nodes", c.Nodes),
		fmt.Sprintf("%d workers", c.Workers),
		fmt.Sprintf("rate %d/s", c.Rate),
		strings.TrimSpace(c.Prep),
		strings.TrimSpace(c.Exec),
	}
}

func (a Config) Merge(b Config) Config {
	if a.Database == "" {
		a.Database = b.Database
	}
	if a.Host == "" {
		a.Host = b.Host
	}
	if a.User == "" {
		a.User = b.User
	}
	if a.Password == "" {
		a.Password = b.Password
	}
	if a.Nodes == 0 {
		a.Nodes = b.Nodes
	}
	if a.Prep == "" {
		a.Prep = b.Prep
	}
	if a.Exec == "" {
		a.Exec = b.Exec
	}
	if len(a.Values) == 0 {
		a.Values = b.Values
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
	return a
}

func (config Config) HostCount() int {
	return len(strings.Split(config.Host, ","))
}
func (config Config) ConnConfig(hostidx int) pgx.ConnConfig {
	hosts := strings.Split(config.Host, ",")

	if hostidx >= len(hosts) {
		return pgx.ConnConfig{}
	}

	host := hosts[hostidx]
	hostport := strings.Split(host, ":")
	port := uint16(26257)
	if len(hostport) > 1 {
		host = hostport[0]
		p, err := strconv.Atoi(hostport[1])
		if err == nil {
			port = uint16(p)
		}
	}

	return pgx.ConnConfig{
		Database: config.Database,
		Host:     host,
		Port:     port,
		User:     config.User,
		Password: config.Password,
	}
}

func getconfigs() ([]Config, error) {

	config := Config{}
	flag.StringVar(&config.Database, "db", DefaultConfig.Database, "Database")
	flag.StringVar(&config.Host, "host", DefaultConfig.Host, "Host")
	flag.StringVar(&config.User, "user", DefaultConfig.User, "User")
	flag.StringVar(&config.Password, "pass", DefaultConfig.Password, "Password")
	flag.IntVar(&config.Workers, "workers", DefaultConfig.Workers, "Workers")
	flag.IntVar(&config.Rate, "rate", DefaultConfig.Rate, "Operations per second")
	flag.DurationVar(&config.Dur, "dur", DefaultConfig.Dur, "Duration")
	flag.Parse()

	configs := []Config{}
	files := flag.Args()
	for _, file := range files {
		c, err := loadconfig(file)
		if err != nil {
			return nil, err
		}
		more := c.Configs
		c.Configs = nil
		c = c.Merge(config)

		if len(more) == 0 {
			configs = append(configs, c)
		}
		for _, m := range more {
			mm := m.Merge(c)
			configs = append(configs, mm)
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
