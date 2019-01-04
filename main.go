package main

import (
	"log"
	"strings"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var colors = []string{
	"dark-violet",
	"#009e73",
	"#56b4e9",
	"#e69f00",
	"#f0e442",
	"#0072b2",
	"#e51e10",
	"black",
}

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

	plot, err := os.Create("plot.pg")
	if err != nil {
		return err
	}
	defer plot.Close()
	fmt.Fprintln(plot, `#!/usr/bin/gnuplot
set terminal pngcairo size 1920,1080 font "verdana,8"
set xlabel "Time"
set ylabel "Latency"
#set format x '%.0s %c'
set tic scale 0
set grid ytics
unset border
set style data lines
set autoscale
set grid
plot \`)

	parts := configs[0].TitleParts()
	show := make([]bool, len(parts))
	for i, config := range configs {
		if i == 0 {
			continue
		}
		p := config.TitleParts()
		for ii, t := range p {
			if t != parts[ii] {
				show[ii] = true
			}
		}
	}

	for i, config := range configs {
		fmt.Println("Running configuration", i+1, "/", len(configs))
		datafilename := fmt.Sprintf("data%d", i)
		var titleparts []string
		for ti, tt := range config.TitleParts() {
			if show[ti] {
				titleparts = append(titleparts, tt)
			}
		}
		title := strings.Join(titleparts, ", ")
		if title == "" {
			title = fmt.Sprintf("config %d", i + 1)
		}
		color := colors[i % len(colors)]
		fmt.Fprintln(plot, plotquote(datafilename)+" using 1:2 title "+plotquote("mean "+title)+" lw 2 dt 1 lc "+plotquote(color)+",\\")
		fmt.Fprintln(plot, plotquote(datafilename)+" using 1:3 title "+plotquote("99% "+title)+" lw 1 dt 2 lc "+plotquote(color)+",\\")

		if err := runconfig(config, datafilename); err != nil {
			return err
		}
	}

	return nil
}

func runconfig(config Config, datafilename string) error {
	done := make(chan struct{})

	fmt.Println(config)

	nodes, err := prepCluster(config, done)
	if err != nil {
		return err
	}

	if err := prepSchema(config); err != nil {
		close(done)
		return err
	}

	errs := make(chan error, config.Workers)
	workerdata := make(chan [][]float64, config.Workers)

	for i := 0; i < config.Workers; i++ {
		go worker(config, done, i, errs, workerdata)
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

	// wait for workers to finish, collecting errors, and
	// aggregating all metrics for final sort and processing
	alldata := [][]float64{}
	for i := 0; i < config.Workers; i++ {
		err := <-errs
		if err != nil {
			log.Println("Worker:", err)
		}
		wd := <-workerdata
		alldata = append(alldata, wd...)
	}

	fmt.Println("workers complete")

	if err := processMetrics(datafilename, alldata); err != nil {
		return err
	}

	if err := waitCluster(nodes); err != nil {
		return err
	}

	// Process through the data:
	// We need to sort by time, do averages, percentiles, etc

	return nil
}
