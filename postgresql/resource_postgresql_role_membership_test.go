package postgresql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccPostgresqlRoleMembership_Basic(t *testing.T) {
	var role string

	rString := acctest.RandStringFromCharSet(10, acctest.CharSetAlpha)
	configBase := fmt.Sprintf(testAccPostgreSQLRoleMemberConfig, rString, rString, rString)
	configUpdate := fmt.Sprintf(testAccPostgreSQLRoleMemberConfigUpdate, rString, rString, rString, rString, rString)
	configUpdateDown := fmt.Sprintf(testAccPostgreSQLRoleMemberConfigUpdateDown, rString, rString, rString)

	testMember := fmt.Sprintf("test-member-%s", rString)
	testMemberTwo := fmt.Sprintf("test-member-two-%s", rString)
	testMemberThree := fmt.Sprintf("test-member-three-%s", rString)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleMembershipDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: configBase,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleMembershipExists("postgresql_role_membership.membership", &role),
					testAccCheckPostgreSQLRoleMembershipAttributes(&role, []string{testMember}),
				),
			},
			resource.TestStep{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleMembershipExists("postgresql_role_membership.membership", &role),
					testAccCheckPostgreSQLRoleMembershipAttributes(&role, []string{testMemberTwo, testMemberThree}),
				),
			},
			resource.TestStep{
				Config: configUpdateDown,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleMembershipExists("postgresql_role_membership.membership", &role),
					testAccCheckPostgreSQLRoleMembershipAttributes(&role, []string{testMemberTwo}),
				),
			},
		},
	})
}

func testAccCheckPostgresqlRoleMembershipDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "postgresql_role" {
			continue
		}
		exists, err := checkRoleMembershipExists(client, rs.Primary.ID)
		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}
		if exists {
			return fmt.Errorf("Role still exists after destroy")
		}
	}

	return nil
}

func testAccCheckPostgresqlRoleMembershipExists(n string, role *string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Resource not found: %s", n)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}
		client := testAccProvider.Meta().(*Client)
		exists, err := checkRoleMembershipExists(client, rs.Primary.Attributes["role"])
		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if !exists {
			return fmt.Errorf("Role not found")
		}
		*role = rs.Primary.Attributes["role"]

		return nil
	}
}

func testAccCheckPostgreSQLRoleMembershipAttributes(role *string, members []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		if !strings.Contains(*role, "test-role") {
			return fmt.Errorf("Bad role membership: expected %s, got %s", "test-role", *role)
		}
		c := testAccProvider.Meta().(*Client)
		var actual_members []string
		roleSQL := fmt.Sprintf("SELECT rolname FROM pg_roles u JOIN pg_group g ON u.oid = ANY(g.grolist) WHERE g.groname=$1;")
		rows, err := c.DB().Query(roleSQL, role)
		if err != nil {
			fmt.Errorf("Error reading info about role: %s", err)
		}
		defer rows.Close()
		for rows.Next() {
			var member string
			if err := rows.Scan(&member); err != nil {
				fmt.Errorf("Error reading info about role: %s", err)
			}
			actual_members = append(actual_members, member)
		}
		err = rows.Err()
		switch {
		case err == sql.ErrNoRows:
			fmt.Errorf("[WARN] PostgreSQL roles belonging to (%s) not found", role)
			return nil
		case err != nil:
			return fmt.Errorf("Error reading ROLE: {{err}}", err)
		}
		memberc := len(members)
		for _, m := range members {
			for _, am := range actual_members {
				if m == am {
					memberc--
				}
			}
		}
		if memberc > 0 {
			return fmt.Errorf("Bad group membership count, expected (%d), but only (%d) found", len(members), memberc)
		}
		return nil
	}
}

func checkRoleMembershipExists(client *Client, roleName string) (bool, error) {
	var _rez int
	err := client.DB().QueryRow("SELECT 1 from pg_roles d WHERE rolname=$1", roleName).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about role: %s", err)
	}
	return true, nil
}

const testAccPostgreSQLRoleMemberConfig = `
resource "postgresql_role" "role" {
    name = "test-role-%s"
}
resource "postgresql_role" "member" {
    name = "test-member-%s"
}
resource "postgresql_role_membership" "membership" {
    name = "tf-testing-role-membership-%s"
    members = ["${postgresql_role.member.name}"]
    role = "${postgresql_role.role.name}"
}
`

const testAccPostgreSQLRoleMemberConfigUpdate = `
resource "postgresql_role" "role" {
    name = "test-role-%s"
}
resource "postgresql_role" "member" {
    name = "test-member-%s"
}
resource "postgresql_role" "member_two" {
    name = "test-member-two-%s"
}
resource "postgresql_role" "member_three" {
    name = "test-member-three-%s"
}
resource "postgresql_role_membership" "membership" {
    name = "tf-testing-role-membership-%s"
    members = [
        "${postgresql_role.member_two.name}",
        "${postgresql_role.member_three.name}",
    ]
    role = "${postgresql_role.role.name}"
}
`

const testAccPostgreSQLRoleMemberConfigUpdateDown = `
resource "postgresql_role" "role" {
    name = "test-role-%s"
}
resource "postgresql_role" "member_three" {
    name = "test-member-three-%s"
}
resource "postgresql_role_membership" "membership" {
    name = "tf-testing-group-membership-%s"
    members = [
        "${postgresql_role.member_three.name}",
    ]
    role = "${postgresql_role.role.name}"
}
`
