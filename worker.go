package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx"
)

const (
	rate_buckets     = 10
	rate_bucket_size = 10
)

var (
	work_mu    sync.Mutex
	work_count int
	work_dur   time.Duration
	work_idle  time.Duration
)

func worker(config Config, done chan struct{}, worker_i int) {
	err := func() error {
		hostcount := config.Nodes // config.HostCount()

		conn, err := pgx.Connect(config.ConnConfig(worker_i % hostcount))
		if err != nil {
			return err
		}
		defer conn.Close()

		// Try not to all activate at the same time
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		// put prep here.

		bucket_times := make([]time.Duration, rate_buckets)
		bucket_count := make([]int, rate_buckets)
		bucket_i := 0

		worker_c := 0
		worker_t := time.Duration(0)
		worker_idle := time.Duration(0)

		for {
			select {
			case _ = <-done:
				return nil
			default:
			}

			start := time.Now()
			ops := 1

			sql := config.Exec
			values := []interface{}{}
			for _, v := range config.Values {
				switch v.Type {
				case "random_uuid":
					id, err := uuid.NewV4()
					if err != nil {
						return err
					}
					values = append(values, id.String())
				case "text":
					values = append(values, v.String)
				default:
					return fmt.Errorf("Unhandled config value type %#v", v.Type)
				}
			}
			//log.Println(debugsql(sql, values))
			if _, err := conn.Exec(sql, values...); err != nil {
				return err
			}

			t := time.Now()
			dur := t.Sub(start)

			work_mu.Lock()
			work_count += ops
			work_dur += dur
			work_idle += worker_idle
			work_mu.Unlock()

			if config.Rate == 0 {
				worker_idle = 0
				continue
			}

			// Rate limiting logic follows. I'm probably overthinking this...

			worker_c += ops
			worker_t += dur

			total_c := worker_c
			total_t := worker_t
			for i, b := range bucket_times {
				total_c += bucket_count[i]
				total_t += b
			}

			if worker_c == rate_bucket_size {
				bucket_times[bucket_i] = worker_t
				bucket_count[bucket_i] = worker_c
				worker_c = 0
				worker_t = 0
				bucket_i++
				if bucket_i == rate_buckets {
					bucket_i = 0
				}
			}

			current := total_t.Seconds() / float64(total_c)
			desired := current
			if config.Rate != 0 {
				d := 1.0 / (float64(config.Rate) / float64(config.Workers))
				if d > desired {
					desired = d
				}
			}
			//if config.WorkerRate != 0 {
			//	d := 1.0 / float64(config.WorkerRate)
			//	if d > desired {
			//		desired = d
			//	}
			//}
			sleep := desired - current
			sleepd := time.Duration(sleep * 1e9)

			//fmt.Println("current", current, "desired", desired, "sleep", sleep, sleepd)

			if sleep > 0 {
				time.Sleep(sleepd)
				worker_idle = sleepd
			} else {
				worker_idle = 0
			}
		}
	}()
	if err != nil {
		log.Printf("Worker error: %v\n", err)
	}
}
