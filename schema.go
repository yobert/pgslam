package main

import (
	"fmt"
	"strings"
)

import (
	"github.com/jackc/pgx"
	"github.com/schollz/progressbar/v2"
)

func prepSchema(config *Config) error {
	conn, err := pgx.Connect(config.ConnConfig())
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.Exec(`
create table if not exists t1 (
	id bigserial not null primary key,
	a timestamptz not null default now(),
	b timestamptz not null default now(),
	c timestamptz not null default now(),
	d timestamptz not null default now(),
	data text
);

create table if not exists t2 (
	id bigserial not null primary key,
	a timestamptz not null default now(),
	b timestamptz not null default now(),
	c timestamptz not null default now(),
	d timestamptz not null default now(),
	data text
);
create index if not exists t2a on t2 (a);

create table if not exists t3 (
	id bigserial not null primary key,
	a timestamptz not null default now(),
	b timestamptz not null default now(),
	c timestamptz not null default now(),
	d timestamptz not null default now(),
	data text
);
create index if not exists t3a on t3 (a);
create index if not exists t3b on t3 (b);

create table if not exists t4 (
	id bigserial not null primary key,
	a timestamptz not null default now(),
	b timestamptz not null default now(),
	c timestamptz not null default now(),
	d timestamptz not null default now(),
	data text
);

create index if not exists t4a on t4 (a);
create index if not exists t4b on t4 (b);
create index if not exists t4c on t4 (c);

create table if not exists t5 (
	id bigserial not null primary key,
	a timestamptz not null default now(),
	b timestamptz not null default now(),
	c timestamptz not null default now(),
	d timestamptz not null default now(),
	data text
);

create index if not exists t5a on t5 (a);
create index if not exists t5b on t5 (b);
create index if not exists t5c on t5 (c);
create index if not exists t5d on t5 (d);
`); err != nil {
		return err
	}

	//	if err := t_load(conn, "t1", 1e6, "insert into t1 (a) select now() from generate_series(1, COUNT);"); err != nil {
	//		return err
	//	}

	return nil
}

func t_load_for(config *Config, table string, ltable string, load string, sql string, args ...interface{}) error {
	conn, err := pgx.Connect(config.ConnConfig())
	if err != nil {
		return err
	}
	defer conn.Close()

	c, err := t_len(conn, table)
	if err != nil {
		return err
	}
	if c > 0 {
		return nil
	}
	fmt.Println("pre-loading table", table, "...")

	cc, err := t_len(conn, ltable)
	if err != nil {
		return err
	}

	var id int

	rows, err := conn.Query(load)
	if err != nil {
		return err
	}

	bar := progressbar.New(cc)

	nargs := []interface{}{nil}
	nargs = append(nargs, args...)

	sqlbuf := ""

	worker_count := 32
	work := make(chan string, worker_count)
	workerr := make(chan error)

	for i := 0; i < worker_count; i++ {
		go func(ii int) {
			//fmt.Println("worker", ii, "started")
			//defer fmt.Println("worker", ii, "done")
			conn2, err := pgx.Connect(config.ConnConfig())
			if err != nil {
				workerr <- err
				return
			}
			defer conn2.Close()
			for {
				s, ok := <-work
				if !ok {
					// done!
					workerr <- nil
					return
				}

				if _, err = conn2.Exec(s); err != nil {
					workerr <- err
					return
				}
			}
		}(i)
	}

	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return err
		}
		nargs[0] = id

		sqlbuf += fillout_query(sql, nargs...) + "\n"

		// 512 bytes of sql at a time
		if len(sqlbuf) > 512 {
			work <- sqlbuf
			sqlbuf = ""
		}

		bar.Add(1)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := bar.Finish(); err != nil {
		return err
	}
	fmt.Println()

	close(work)

	fmt.Print("waiting for workers: ")

	// wait for workers to finish or report errors
	var anyerr error
	for i := 0; i < worker_count; i++ {
		err := <-workerr
		fmt.Print(".")
		if err != nil {
			anyerr = err
		}
	}
	fmt.Println()
	if anyerr != nil {
		return anyerr
	}
	return nil
}

func fillout_query(sql string, args ...interface{}) string {
	for i, a := range args {
		switch v := a.(type) {
		case int:
			sql = strings.Replace(sql, fmt.Sprintf("$%d", i+1), fmt.Sprintf("%d", v), -1)
		case string:
			sql = strings.Replace(sql, fmt.Sprintf("$%d", i+1), quotestring(v), -1)
		}
	}
	return sql
}

func quotestring(str string) string {
	return "'" + strings.Replace(str, "'", "''", -1) + "'"
}

func t_load(conn *pgx.Conn, table string, count int, sql string, args ...interface{}) error {
	c, err := t_len(conn, table)
	if err != nil {
		return err
	}
	if c > 0 {
		return nil
	}
	fmt.Print("pre-loading table ", table, " ... ")
	if _, err := conn.Exec(sql, args...); err != nil {
		return err
	}
	fmt.Println("done")
	return nil
}

func t_len(conn *pgx.Conn, table string) (int, error) {
	r := 0
	if err := conn.QueryRow(`select count(1) from ` + table + `;`).Scan(&r); err != nil {
		return 0, err
	}
	return r, nil
}
