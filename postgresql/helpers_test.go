package postgresql

import (
	"testing"
)

func TestBadURL(t *testing.T) {
	_, err := parseConnectionString("test")

	if err == nil {
		t.Error("Expected bad URL to return error")
	}
}

func TestBadScheme(t *testing.T) {
	_, err := parseConnectionString("notpostgres://test")

	if err == nil {
		t.Error("Expected bad schema to return error")
	}
}

func TestURLParsing(t *testing.T) {
	expectations := map[string]postgresConnString{
		"postgres://testhost/testdb": postgresConnString{
			netloc: "testhost", dbname: "testdb",
		},
		"postgresql://testhost/testdb": postgresConnString{
			netloc: "testhost", dbname: "testdb",
		},
		"postgres://testhost": postgresConnString{netloc: "testhost"},
		"postgres://user@testhost": postgresConnString{
			netloc: "testhost", username: "user",
		},
		"postgres://user:pass@testhost": postgresConnString{
			netloc: "testhost", username: "user", password: "pass",
		},
		"postgres://testhost:1234": postgresConnString{
			netloc: "testhost", port: 1234,
		},
		"postgres://user:pass@testhost:1234/dbname": postgresConnString{
			netloc: "testhost", port: 1234, username: "user", password: "pass",
			dbname: "dbname",
		},
	}

	for k, v := range expectations {
		result, err := parseConnectionString(k)

		if err != nil {
			t.Error("Unexpected error parsing ", k)
		}

		if v != result {
			t.Error("Unexpected result parsing ", k, v, result)
		}
	}
}
