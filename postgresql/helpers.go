package postgresql

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// pqQuoteLiteral returns a string literal safe for inclusion in a PostgreSQL
// query as a parameter.  The resulting string still needs to be wrapped in
// single quotes in SQL (i.e. fmt.Sprintf(`'%s'`, pqQuoteLiteral("str"))).  See
// quote_literal_internal() in postgresql/backend/utils/adt/quote.c:77.
func pqQuoteLiteral(in string) string {
	in = strings.Replace(in, `\`, `\\`, -1)
	in = strings.Replace(in, `'`, `''`, -1)
	return in
}

func validateConnLimit(v interface{}, key string) (warnings []string, errors []error) {
	value := v.(int)
	if value < -1 {
		errors = append(errors, fmt.Errorf("%s can not be less than -1", key))
	}
	return
}

// libPQ style connection strings are:
// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
// The only param we care about is sslMode
type postgresConnString struct {
	username string
	password string
	netloc   string
	port     int
	dbname   string
	sslmode  string
}

func parseConnectionString(connString string) (postgresConnString, error) {
	url, err := url.Parse(connString)

	if err != nil {
		return postgresConnString{}, err
	}

	if url.Scheme != "postgres" && url.Scheme != "postgresql" {
		return postgresConnString{}, errors.New("Not a PostgreSQL URL")
	}

	username, password := usernameAndPasswordFromURL(url)
	netloc, port := hostAndPortFromURL(url)
	dbname := dbnameFromURL(url)
	sslmode := sslmodeFromURL(url)

	r := postgresConnString{username, password, netloc, port, dbname, sslmode}

	return r, nil
}

func usernameAndPasswordFromURL(url *url.URL) (string, string) {
	var username string
	var password string

	if url.User != nil {
		username = url.User.Username()
		password, _ = url.User.Password()
	}

	return username, password
}

func sslmodeFromURL(url *url.URL) string {
	var sslmode string

	queryVals := url.Query()

	if len(queryVals["sslmode"]) == 1 {
		sslmode = queryVals["sslmode"][0]
	}

	return sslmode
}

func hostAndPortFromURL(url *url.URL) (string, int) {
	parts := strings.Split(url.Host, ":")

	if len(parts) == 1 {
		return parts[0], 0
	} else {
		port, _ := strconv.Atoi(parts[1])
		return parts[0], port
	}
}

func dbnameFromURL(url *url.URL) string {
	path := url.Path

	return strings.TrimPrefix(path, "/")
}
