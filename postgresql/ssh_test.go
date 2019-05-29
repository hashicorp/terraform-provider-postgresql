package postgresql

import (
	"github.com/hashicorp/terraform/helper/resource"
	"testing"
)

func TestAccPostgresqlSsh_Connect(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheckSsh(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: "",
				Check:  resource.ComposeTestCheckFunc(),
			},
		},
	})
}

func TestAccPostgresqlSshDatabase_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheckSsh(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlDatabaseDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgreSQLDatabaseConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlDatabaseExists("postgresql_database.mydb"),
				),
			},
		},
	})
}

// TODO add documentation of the ssh tunnel feature
