package postgresql

import (
	"github.com/hashicorp/terraform/config"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
	"os"
	"testing"
)

var testAccProviders map[string]terraform.ResourceProvider
var testAccProvider *schema.Provider
var testAccSshProvider *schema.Provider

func init() {
	testAccProvider = Provider().(*schema.Provider)
	testAccSshProvider = Provider().(*schema.Provider)

	testAccProviders = map[string]terraform.ResourceProvider{
		"postgresql":     testAccProvider,
		"postgresql+ssh": testAccSshProvider,
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().(*schema.Provider).InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ terraform.ResourceProvider = Provider()
}

func testAccPreCheck(t *testing.T) {
	var host string
	if host = os.Getenv("PGHOST"); host == "" {
		t.Fatal("PGHOST must be set for acceptance tests")
	}
	if v := os.Getenv("PGUSER"); v == "" {
		t.Fatal("PGUSER must be set for acceptance tests")
	}

	err := testAccProvider.Configure(terraform.NewResourceConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
}

func testAccPreCheckSsh(t *testing.T) {
	//var host string
	//if host = os.Getenv("PGHOST"); host == "" {
	//	t.Fatal("PGHOST must be set for acceptance tests")
	//}

	if v := os.Getenv("PGUSER"); v == "" {
		t.Fatal("PGUSER must be set for acceptance tests")
	}

	bastionPrivateKey := `
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABFwAAAAdzc2gtcn
NhAAAAAwEAAQAAAQEAu5wYi5SxCTcmChVWaS34MYV25GC0eyrhPLt54lmBNHmA+088a038
azBMs8/XxYcMpcIqE92UBXuMXRe230leV36t0Qw0n3/eg/OP9ctSCOD1pISjsLFSi5UTp9
dbUlePYbYmbsRm14+3vhGXlOLUc6ApfdO4wn13NSi/zQVrEtlz15GUNWmgFKPfFHNTlQDO
QKKpMgNKeUxbeq1kfrA4b/9PwZyaQcQn84SqNAOYTuy8ZqnC7A6yRJFLfnqxOu6TYUkO3V
/SH7oivh02bCgKMCTi1R8z0qLwz+vn+xRYb3ysDxUzqMSoZ8rLBXjnrF9xv3Uoq0b6bfbK
lmhywslkxwAAA9AXdYZFF3WGRQAAAAdzc2gtcnNhAAABAQC7nBiLlLEJNyYKFVZpLfgxhX
bkYLR7KuE8u3niWYE0eYD7TzxrTfxrMEyzz9fFhwylwioT3ZQFe4xdF7bfSV5Xfq3RDDSf
f96D84/1y1II4PWkhKOwsVKLlROn11tSV49htiZuxGbXj7e+EZeU4tRzoCl907jCfXc1KL
/NBWsS2XPXkZQ1aaAUo98Uc1OVAM5AoqkyA0p5TFt6rWR+sDhv/0/BnJpBxCfzhKo0A5hO
7LxmqcLsDrJEkUt+erE67pNhSQ7dX9IfuiK+HTZsKAowJOLVHzPSovDP6+f7FFhvfKwPFT
OoxKhnyssFeOesX3G/dSirRvpt9sqWaHLCyWTHAAAAAwEAAQAAAQEArrpRjeYc/9UyA2Ae
C3V52z1PHqIGVVP5VGPSv4HWuPWUr/n67oFCXt4sAafIcLo3iEWOhNPwIS8Q6j7E3a5qRB
jCb5jrhcVEiyYTZLtJGuXRQbka7twnYcKk/MOw1L6h1kIcBzu6AHdkjIu73jln3oxDOGIw
iErr9EGQaLTsJS9xXFD7R8opqNNTb7uQHGbDux5TWXSLNRtUhi/m/i+tcPBf+edhT/I0lP
9msEzIxhCjr+1/M9yQgsLIs2pyXYRlkBs2J3ZIo5PuF+SclOC7YudorA8g/KbX1nbTK84Z
MIrvBIjQxcPQ9rGHow2tOy2fQDsErj9H61RrvjkCIWC3KQAAAIALFVGwKDQX82jAOjaLAy
NUCYPYQPJ3XfSITyh/59SOexDkNxY2IpSx1cD6FrJUkqbKAbJ9PgqA18aAauQnaMpIUJ4Y
128iVH7H2AgrQPFNpgbRj7lIsn6Y9W6Uj5PA3jQHepypaT3S+F4HhcD15S7V2bi9olBmYv
2O+fKSNH47KwAAAIEA7uhpTBM9C6CXKoFu2FISKLaJ63gk0Z32ezyRyv2KH2ctJV1psuqd
MiaihieGg+6tXuth/EoMWteagDG+845G/BA4OOMV1vidAf2MhzmkO4XPRGJ3eFIaHhXl5i
mIhcDsJWQbZZ3lInWOyGgP2GmLt/1WlBVa/ueYfnkXeMmYEysAAACBAMkIKV6oPTmGCu8w
CxaFi27NPbGtVlfVUqxTLXUqb4KR6I3xdzNkX89dMUciY3ty0U6KHlyvuZ8NbHOS0DFRAj
kEUUUYbBzkLkM1RxzAWiy+BueGdxNZnPuFqi5qsgguHPhMe0+2H4TlQn9J3pRK5bM3SoK2
j6FW0DcOmqu981bVAAAAGmxla3NlQERvbWluaWtzLU1CUC0yLmxvY2Fs
-----END OPENSSH PRIVATE KEY-----
`

	c := map[string]interface{}{
		"host": "postgres",
		"port": 5432,
		"connection": []map[string]interface{}{
			{
				"bastion_host":        "localhost",
				"bastion_port":        20022,
				"bastion_user":        "test",
				"bastion_private_key": bastionPrivateKey,
			},
		},
	}

	rc, err := config.NewRawConfig(c)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	err = testAccSshProvider.Configure(terraform.NewResourceConfig(rc))
	if err != nil {
		t.Fatal(err)
	}
}
