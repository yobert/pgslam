package main

import (
	"fmt"
	//	"math/rand"
	"os"
	"time"
	//	"github.com/jackc/pgx"
	//	"github.com/schollz/progressbar"
)

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	configs := []*Config{}

	for _, filename := range os.Args[1:] {
		config, err := loadconfig(filename)
		if err != nil {
			return err
		}
		configs = append(configs, config)
	}

	for i, config := range configs {
		fmt.Println("Running configuration", i+1, "/", len(configs))
		if err := runconfig(config); err != nil {
			return err
		}
	}

	return nil
}

func runconfig(config *Config) error {
	fmt.Println("schema...")
	if err := prepSchema(config); err != nil {
		return err
	}

	done := make(chan struct{})

	for i := 0; i < config.Workers; i++ {
		go worker(config, done)
	}

	// give them a second to prime and connect
	fmt.Println("priming...")
	time.Sleep(time.Second * 2)

	work_mu.Lock()
	work_count = 0
	work_dur = 0
	work_mu.Unlock()

	start := time.Now()
	last_count := 0
	last_dur := time.Duration(0)
	t := time.Now()

	for {
		time.Sleep(time.Second)

		work_mu.Lock()
		c := work_count
		d := work_dur
		work_mu.Unlock()

		tt := time.Now()

		fmt.Printf("%.2f/s %s/op\n",
			float64(c-last_count)/tt.Sub(t).Seconds(),
			(d-last_dur)/time.Duration(c-last_count),
		)
		t = tt
		last_count = c
		last_dur = d

		if time.Since(start) > config.Dur {
			break
		}
	}
	close(done)

	return nil
}
