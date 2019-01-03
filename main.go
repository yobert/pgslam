package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	rand.Seed(time.Now().Unix())

	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	configs, err := getconfigs()
	if err != nil {
		return err
	}

	for i, config := range configs {
		fmt.Println("Running configuration", i+1, "/", len(configs))
		if err := runconfig(config); err != nil {
			return err
		}
	}

	return nil
}

func runconfig(config Config) error {
	done := make(chan struct{})

	fmt.Println(config)

	if err := prepCluster(config, done); err != nil {
		return err
	}

	if err := prepSchema(config); err != nil {
		return err
	}

	for i := 0; i < config.Workers; i++ {
		go worker(config, done, i)
	}

	time.Sleep(time.Second * 2)

	work_mu.Lock()
	work_count = 0
	work_dur = 0
	work_idle = 0
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
		wi := work_idle
		work_idle = 0
		work_mu.Unlock()

		tt := time.Now()

		s := tt.Sub(t).Seconds()
		is := wi.Seconds()
		ir := (1.0 - (is / s / float64(config.Workers))) * 100.0

		if c-last_count > 0 {
			rate := float64(c-last_count) / s
			speed := (d - last_dur) / time.Duration(c-last_count)
			speed = speed.Truncate(time.Microsecond)

			fmt.Printf("%.2f/s %s/op (%.0f%%)\n",
				rate,
				speed,
				ir,
			)
		} else {
			fmt.Printf("-----/s -/op (%.0f%%)\n",
				ir)
		}
		t = tt
		last_count = c
		last_dur = d

		if config.Dur != 0 && time.Since(start) > config.Dur {
			break
		}
	}
	close(done)

	if err := waitCluster(); err != nil {
		return err
	}

	return nil
}
