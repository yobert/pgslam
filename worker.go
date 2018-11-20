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
)

func worker(config *Config, done chan struct{}) {
	conn, err := pgx.Connect(config.ConnConfig())
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	// Try not to all activate at the same time
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

	//var stuff []time.Time
	var stuff []string
	if config.Op == "select" || config.Op == "join" {
		tab := config.Table
		col := config.Column
		if config.Op == "join" {
			tab = "calendars"
			col = "calendar_id"
		}

		rows, err := conn.Query(`select ` + col + `::text from ` + tab + ` limit 1000;`)
		if err != nil {
			fmt.Println(err)
			return
		}
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				fmt.Println(err)
				return
			}
			stuff = append(stuff, s)
		}
		if err := rows.Err(); err != nil {
			fmt.Println(err)
			return
		}
	}

	bucket_times := make([]time.Duration, rate_buckets)
	bucket_count := make([]int, rate_buckets)
	bucket_i := 0

	worker_c := 0
	worker_t := time.Duration(0)

	for {
		select {
		case _ = <-done:
			return
		default:
		}

		start := time.Now()

		switch config.Op {
		case "insert":
			if _, err := conn.Exec(`insert into `+config.Table+` (data) values ($1);`, "some crap"); err != nil {
				log.Println(err)
				return
			}
		case "select":
			var id int
			err := conn.QueryRow(`select id from `+config.Table+` where `+config.Column+` = $1 limit 1;`, stuff[rand.Intn(len(stuff))]).Scan(&id)
			if err != nil {
				log.Println(err)
				return
			}
		case "join":
			res := struct {
				CompanyID  int
				UserID     int
				CalendarID int
				EventID    int
			}{}

			err := conn.QueryRow(`
select
	c.company_id,
	u.user_id,
	ca.calendar_id,
	e.event_id
from companies as c
inner join users as u on u.company_id = c.company_id
inner join calendars as ca on ca.user_id = u.user_id
inner join events as e on e.calendar_id = ca.calendar_id
where ca.calendar_id = $1 and e.created_at > now() - $2::interval;`,
				stuff[rand.Intn(len(stuff))],
				"1 day",
			).Scan(&res.CompanyID, &res.UserID, &res.CalendarID, &res.EventID)
			if err != nil {
				log.Println(err)
				return
			}
		default:
			log.Printf("Unknown worker operation %#v\n", config.Op)
			return
		}

		t := time.Now()
		dur := t.Sub(start)

		work_mu.Lock()
		work_count++
		work_dur += dur
		work_mu.Unlock()

		if config.Rate == 0 && config.WorkerRate == 0 {
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
		}
	}
}
