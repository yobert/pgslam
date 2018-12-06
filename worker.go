package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

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
	work_idle time.Duration
)

func worker(config *Config, done chan struct{}) {
	err := func() error {

	conn, err := pgx.Connect(config.ConnConfig())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Try not to all activate at the same time
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

	var (
		company_id int
		stuff      []string
		sql        string
		args       []interface{}
	)

	if config.Op == "select" || config.Op == "update" {
		sql = `select ` + config.Column + `::text from ` + config.Table + ` order by random() limit 1000;`
	} else if config.Op == "join" {
		rows, err := conn.Query(`select company_id from calendar group by 1 order by random() limit 1;`)
		if err != nil {
			return fmt.Errorf("finding company_id: %v", err)
		}
		for rows.Next() {
			if err := rows.Scan(&company_id); err != nil {
				return fmt.Errorf("scanning company_id: %v", err)
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("reading company_id: %v", err)
		}
		//sql = `select id from calendar where company_id = $1 order by random() limit 1000;`
		sql = `select id from reservation where company_id = $1 order by random() limit 10000;`
		args = append(args, company_id)
	}

	rows, err := conn.Query(sql, args...)
	if err != nil {
		return fmt.Errorf("query stuff: %v", err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return fmt.Errorf("scanning stuff: %v", err)
		}
		stuff = append(stuff, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("finishing stuff: %v", err)
	}

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

		switch config.Op {
		case "insert":
			if _, err := conn.Exec(`insert into `+config.Table+` (data) values ($1);`, "some crap"); err != nil {
				return err
			}
		case "select":
			var id int
			err := conn.QueryRow(`select id from `+config.Table+` where `+config.Column+` = $1 limit 1;`, stuff[rand.Intn(len(stuff))]).Scan(&id)
			if err != nil {
				return err
			}
		case "update":
			if _, err := conn.Exec(`update `+config.Table+` set data = $1 where `+config.Column+` = $2;`, "updated value", stuff[rand.Intn(len(stuff))]); err != nil {
				return err
			}
		case "join":
			pemail := ""

			//sql := `select p.email from reservation as r inner join participant as p on p.company_id = r.company_id and p.reservation_id = r.id where r.company_id = $1 and r.calendar_id = $2;`
			sql := `select p.email from participant as p where p.company_id = $1 and p.reservation_id = $2`
			args := []interface{}{
				company_id,
				stuff[rand.Intn(len(stuff))],
			}

			//fmt.Println(debugsql(sql, args))

			err := conn.QueryRow(sql, args...).Scan(
				&pemail,
			)
			if err != nil {
				return fmt.Errorf("scan %v", err)
			}
		default:
			return fmt.Errorf("Unknown worker operation %#v\n", config.Op)
		}

		t := time.Now()
		dur := t.Sub(start)

		work_mu.Lock()
		work_count++
		work_dur += dur
		work_idle += worker_idle
		work_mu.Unlock()

		if config.Rate == 0 && config.WorkerRate == 0 {
			worker_idle = 0
			continue
		}

		// Rate limiting logic follows. I'm probably overthinking this...

		worker_c++
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
		if config.WorkerRate != 0 {
			d := 1.0 / float64(config.WorkerRate)
			if d > desired {
				desired = d
			}
		}
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
