package main

import (
	"github.com/jackc/pgx"
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
	return nil
}
