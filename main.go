package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
)

const (
	workers = 800
	inserts = 100
	selects = 100
	updates = 10000
	deletes = 100

	preload      = 10000000
	preloadBatch = 10000

	slew = 10000

	table = `garbage`
)

var (
	insertCount int64
	selectCount int64
	updateCount int64
	deleteCount int64
	workerCount int64

	config = pgx.ConnConfig{
		Database: "sup",
	}
)

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	conn, err := pgx.Connect(config)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Println("database close error:", err)
		}
	}()

	if _, err := conn.Exec(`create table if not exists ` + table + ` (id serial not null primary key, name text);`); err != nil {
		return err
	}

	var count int
	if err := conn.QueryRow(`select count(1) from ` + table + `;`).Scan(&count); err != nil {
		return err
	}
	preloadLeft := preload - count
	if preloadLeft < 0 {
		preloadLeft = 0
	}

	fmt.Printf("%d existing rows (%d more to preload)\n", count, preloadLeft)

	workersDone := make(chan struct{}, workers)
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	go stats(done)

	for i := 0; i < workers; i++ {
		go func(ii int) {
			err := worker(ii, done, preloadLeft/workers)
			if err != nil {
				fmt.Println("worker", ii, "error:", err)
			}
			workersDone <- struct{}{}
		}(i)
	}

	_ = <-interrupt
	close(done)

	for i := 0; i < workers; i++ {
		_ = <-workersDone
	}
	return nil
}

func worker(idx int, done chan struct{}, preloadLeft int) error {
	time.Sleep(time.Duration(rand.Intn(slew)) * time.Millisecond)
	atomic.AddInt64(&workerCount, 1)

	var (
		loop, i int
	)

	conn, err := pgx.Connect(config)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Println("worker", idx, "database close error:", err)
		}
	}()

	i = 0
	for i < preloadLeft {
		select {
		case <-done:
			return nil
		default:
		}

		if _, err := conn.Exec(`insert into `+table+` (name) select $1 || i::text from generate_series(0, $2) as t(i);`,
			fmt.Sprintf("worker %d preloaded row ", idx), preloadBatch); err != nil {
			return err
		}
		i += preloadBatch
		atomic.AddInt64(&insertCount, preloadBatch)
	}

	for {
		stuff := make(map[int]string)

		for i = 0; i < inserts; i++ {

			select {
			case <-done:
				return nil
			default:
			}

			var id int
			name := fmt.Sprintf("worker %d loop %d insert %d", idx, loop, i)
			err := conn.QueryRow(`insert into `+table+` (name) values ($1) returning id;`, name).Scan(&id)
			if err != nil {
				return err
			}

			atomic.AddInt64(&insertCount, 1)

			stuff[id] = name
		}

		i = 0
	selects:
		for i < selects {
			for id, name := range stuff {

				select {
				case <-done:
					return nil
				default:
				}

				if i >= selects {
					break selects
				}

				var n string
				err := conn.QueryRow(`select name from `+table+` where id = $1;`, id).Scan(&n)
				if err != nil {
					return err
				}
				if n != name {
					return fmt.Errorf("Oh shit: id %d was supposed to be %#v but we got %#v", id, name, n)
				}

				atomic.AddInt64(&selectCount, 1)

				i++
			}
		}

		i = 0
	updates:
		for i < updates {
			for id := range stuff {

				select {
				case <-done:
					return nil
				default:
				}

				if i >= updates {
					break updates
				}

				name := fmt.Sprintf("worker %d loop %d update %d", idx, loop, i)
				_, err := conn.Exec(`update `+table+` set name = $1 where id = $2;`, name, id)
				if err != nil {
					return err
				}
				stuff[id] = name

				atomic.AddInt64(&updateCount, 1)
				i++
			}
		}

		i = 0
		for id := range stuff {

			select {
			case <-done:
				return nil
			default:
			}

			if i >= deletes {
				break
			}

			_, err := conn.Exec(`delete from `+table+` where id = $1;`, id)
			if err != nil {
				return err
			}

			atomic.AddInt64(&deleteCount, 1)

			i++
		}

		loop++
	}
}

func stats(done chan struct{}) {
	line := ""
	bs := ""

	var (
		lasti int64
		lasts int64
		lastu int64
		lastd int64
		lastq int64
	)
	lastt := time.Now()

	for {
		select {
		case <-done:
			fmt.Println()
			return
		default:
		}

		time.Sleep(time.Millisecond * 1000)

		for len(bs) < len(line) {
			bs = bs + "\b"
		}
		fmt.Print(bs[0:len(line)])

		i := atomic.LoadInt64(&insertCount)
		s := atomic.LoadInt64(&selectCount)
		u := atomic.LoadInt64(&updateCount)
		d := atomic.LoadInt64(&deleteCount)
		q := i + s + u + d
		t := time.Now()
		delta := t.Sub(lastt).Seconds()
		wc := atomic.LoadInt64(&workerCount)

		line = fmt.Sprintf("%10.0f i %10.0f s %10.0f u %10.0f d %10.0f q / second (%d workers)",
			float64(i-lasti)/delta,
			float64(s-lasts)/delta,
			float64(u-lastu)/delta,
			float64(d-lastd)/delta,
			float64(q-lastq)/delta,
			wc,
		)

		fmt.Print(line)

		lasti = i
		lasts = s
		lastu = u
		lastd = d
		lastq = q
		lastt = t
	}
}
