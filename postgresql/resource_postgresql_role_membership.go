package postgresql

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourcePostgreSQLRoleMembership() *schema.Resource {
	return &schema.Resource{
		Create: resourcePostgreSQLRoleMembershipCreate,
		Read:   resourcePostgreSQLRoleMembershipRead,
		Update: resourcePostgreSQLRoleMembershipUpdate,
		Delete: resourcePostgreSQLRoleMembershipDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the role membership",
			},
			"role": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the group role members belong to",
			},
			"members": {
				Type:     schema.TypeSet,
				Required: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Set:      schema.HashString,
			},
		},
	}
}

func resourcePostgreSQLRoleMembershipCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	role := d.Get("role").(string)
	membersList := expandStringList(d.Get("members").(*schema.Set).List())
	if err := addMembersToRole(c, membersList, role); err != nil {
		return err
	}
	d.SetId(d.Get("name").(string))
	return resourcePostgreSQLRoleMembershipRead(d, meta)
}

func resourcePostgreSQLRoleMembershipDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	role := d.Get("role").(string)
	membersList := expandStringList(d.Get("members").(*schema.Set).List())
	if err := removeMembersFromRole(c, membersList, role); err != nil {
		return err
	}

	return nil
}

func resourcePostgreSQLRoleMembershipRead(d *schema.ResourceData, meta interface{}) error {
	return resourcePostgreSQLRoleReadImpl(d, meta)
}

func resourcePostgreSQLRoleMembershipReadImpl(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	role := d.Get("role").(string)
	var members []string

	roleSQL := fmt.Sprintf("SELECT rolname FROM pg_roles u JOIN pg_group g ON u.oid = ANY(g.grolist) WHERE g.groname=$1;")
	rows, err := c.DB().Query(roleSQL, role)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			log.Fatal(err)
		}
		members = append(members, member)
	}
	err = rows.Err()
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL roles belonging to (%s) not found", role)
		d.SetId("")
		return nil
	case err != nil:
		return errwrap.Wrapf("Error reading ROLE: {{err}}", err)
	}

	d.Set("role", role)
	if err := d.Set("members", members); err != nil {
		return fmt.Errorf("[WARN] Error setting role memberss from PostgreSQL role (%s), error: %s", role, err)
	}

	return nil
}

func resourcePostgreSQLRoleMembershipUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	if d.HasChange("members") {
		role := d.Get("role").(string)

		o, n := d.GetChange("members")
		if o == nil {
			o = new(schema.Set)
		}
		if n == nil {
			n = new(schema.Set)
		}

		os := o.(*schema.Set)
		ns := n.(*schema.Set)
		remove := expandStringList(os.Difference(ns).List())
		add := expandStringList(ns.Difference(os).List())

		if err := removeMembersFromRole(c, remove, role); err != nil {
			return err
		}

		if err := addMembersToRole(c, add, role); err != nil {
			return err
		}
	}

	return resourcePostgreSQLRoleReadImpl(d, meta)
}

func removeMembersFromRole(c *Client, membersList []string, role string) error {
	for _, member := range membersList {
		sql := fmt.Sprintf("REVOKE \"%s\" FROM \"%s\"", role, member)
		if _, err := c.DB().Exec(sql); err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error revoking %s from %s: {{err}}", role, member), err)
		}
	}
	return nil
}

func addMembersToRole(c *Client, membersList []string, role string) error {
	for _, member := range membersList {
		sql := fmt.Sprintf("GRANT \"%s\" TO \"%s\"", role, member)
		if _, err := c.DB().Exec(sql); err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error granting %s to role %s: {{err}}", role, member), err)
		}
	}
	return nil
}
