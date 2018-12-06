package main

import (
	"strings"
	"strconv"
	"fmt"
	"os"
	"regexp"
)

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
      default:
         fmt.Fprintf(os.Stderr, "placeholder %#v has unhandled type %T\n", s, v)
         return s
      }
   })
}

func quotestring(str string) string {
   return "'" + strings.Replace(str, "'", "''", -1) + "'"
}
