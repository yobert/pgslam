package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/alessio/shellescape"
	"github.com/jackc/pgx"
)

type pgxlogger int

func (l pgxlogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	log.Println(level, msg, data)
}

var holderRe = regexp.MustCompile(`\$\d+`)

func debugsql(sql string, params []interface{}) string {
	return holderRe.ReplaceAllStringFunc(sql, func(s string) string {
		i, err := strconv.Atoi(s[1:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "placeholder %#v parse error: %v\n", s, err)
			return s
		}
		i--
		if i < 0 || i+1 > len(params) {
			fmt.Fprintf(os.Stderr, "placeholder %#v out of range\n", s)
			return s
		}
		v := params[i]
		switch vv := v.(type) {
		case nil:
			return "null"
		case string:
			return quotestring(vv)
		case int:
			return strconv.Itoa(vv)
		case bool:
			return strconv.FormatBool(vv)
		default:
			fmt.Fprintf(os.Stderr, "placeholder %#v has unhandled type %T\n", s, v)
			return s
		}
	})
}

func quotestring(str string) string {
	return "'" + strings.Replace(str, "'", "''", -1) + "'"
}

func debugcmd(cmd string, args []string) string {
	quoted := []string{cmd}
	for _, a := range args {
		quoted = append(quoted, shellescape.Quote(a))
	}
	return strings.Join(quoted, " ")
}
