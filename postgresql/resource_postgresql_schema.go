package postgresql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/lib/pq"
	acl "github.com/sean-/postgresql-acl"
)

const (
	schemaNameAttr     = "name"
	schemaDatabaseAttr = "database"
	schemaOwnerAttr    = "owner"
	schemaPolicyAttr   = "policy"
	schemaIfNotExists  = "if_not_exists"
	schemaDropCascade  = "drop_cascade"

	schemaPolicyCreateAttr          = "create"
	schemaPolicyCreateWithGrantAttr = "create_with_grant"
	schemaPolicyRoleAttr            = "role"
	schemaPolicyUsageAttr           = "usage"
	schemaPolicyUsageWithGrantAttr  = "usage_with_grant"
)

func resourcePostgreSQLSchema() *schema.Resource {
	return &schema.Resource{
		Create: resourcePostgreSQLSchemaCreate,
		Read:   resourcePostgreSQLSchemaRead,
		Update: resourcePostgreSQLSchemaUpdate,
		Delete: resourcePostgreSQLSchemaDelete,
		Exists: resourcePostgreSQLSchemaExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			schemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the schema",
			},
			schemaDatabaseAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The database name to alter schema",
			},
			schemaOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The ROLE name who owns the schema",
			},
			schemaIfNotExists: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "When true, use the existing schema if it exists",
			},
			schemaDropCascade: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "When true, will also drop all the objects that are contained in the schema",
			},
			schemaPolicyAttr: {
				Type:     schema.TypeSet,
				Optional: true,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						schemaPolicyCreateAttr: {
							Type:          schema.TypeBool,
							Optional:      true,
							Default:       false,
							Description:   "If true, allow the specified ROLEs to CREATE new objects within the schema(s)",
							ConflictsWith: []string{schemaPolicyAttr + "." + schemaPolicyCreateWithGrantAttr},
						},
						schemaPolicyCreateWithGrantAttr: {
							Type:          schema.TypeBool,
							Optional:      true,
							Default:       false,
							Description:   "If true, allow the specified ROLEs to CREATE new objects within the schema(s) and GRANT the same CREATE privilege to different ROLEs",
							ConflictsWith: []string{schemaPolicyAttr + "." + schemaPolicyCreateAttr},
						},
						schemaPolicyRoleAttr: {
							Type:        schema.TypeString,
							Elem:        &schema.Schema{Type: schema.TypeString},
							Optional:    true,
							Default:     "",
							Description: "ROLE who will receive this policy (default: PUBLIC)",
						},
						schemaPolicyUsageAttr: {
							Type:          schema.TypeBool,
							Optional:      true,
							Default:       false,
							Description:   "If true, allow the specified ROLEs to use objects within the schema(s)",
							ConflictsWith: []string{schemaPolicyAttr + "." + schemaPolicyUsageWithGrantAttr},
						},
						schemaPolicyUsageWithGrantAttr: {
							Type:          schema.TypeBool,
							Optional:      true,
							Default:       false,
							Description:   "If true, allow the specified ROLEs to use objects within the schema(s) and GRANT the same USAGE privilege to different ROLEs",
							ConflictsWith: []string{schemaPolicyAttr + "." + schemaPolicyUsageAttr},
						},
					},
				},
			},
		},
	}
}

func resourcePostgreSQLSchemaCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	queries := []string{}

	database := getDatabase(d, c)

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)

	// Check if previous tasks haven't already create schema
	var foundSchema bool
	err = txn.QueryRow(`SELECT TRUE FROM pg_catalog.pg_namespace WHERE nspname = $1`, schemaName).Scan(&foundSchema)

	if err == sql.ErrNoRows {
		b := bytes.NewBufferString("CREATE SCHEMA ")
		if c.featureSupported(featureSchemaCreateIfNotExist) {
			if v := d.Get(schemaIfNotExists); v.(bool) {
				fmt.Fprint(b, "IF NOT EXISTS ")
			}
		}
		fmt.Fprint(b, pq.QuoteIdentifier(schemaName))

		if schemaOwner != "" {
			fmt.Fprint(b, " AUTHORIZATION ", pq.QuoteIdentifier(schemaOwner))
		}
		queries = append(queries, b.String())
	} else {
		if err := setSchemaOwner(txn, d); err != nil {
			return err
		}
	}

	// ACL objects that can generate the necessary SQL
	type RoleKey string
	var schemaPolicies map[RoleKey]acl.Schema

	if policiesRaw, ok := d.GetOk(schemaPolicyAttr); ok {
		policiesList := policiesRaw.(*schema.Set).List()

		// NOTE: len(policiesList) doesn't take into account multiple
		// roles per policy.
		schemaPolicies = make(map[RoleKey]acl.Schema, len(policiesList))

		for _, policyRaw := range policiesList {
			policyMap := policyRaw.(map[string]interface{})
			rolePolicy := schemaPolicyToACL(policyMap)

			roleKey := RoleKey(strings.ToLower(rolePolicy.Role))
			if existingRolePolicy, ok := schemaPolicies[roleKey]; ok {
				schemaPolicies[roleKey] = existingRolePolicy.Merge(rolePolicy)
			} else {
				schemaPolicies[roleKey] = rolePolicy
			}
		}
	}

	for _, policy := range schemaPolicies {
		queries = append(queries, policy.Grants(schemaName)...)
	}

	// Needed in order to set the owner of the schema if the connection user is not a
	// superuser
	currentUser := c.config.getDatabaseUsername()
	ownerGranted := false
	if schemaOwner != "" {
		ownerGranted, err = grantRoleMembership(txn, schemaOwner, currentUser)
		if err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error granting owner membership for schema %s: {{err}}", schemaName), err)
		}
	}

	for _, query := range queries {
		if _, err = txn.Exec(query); err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error creating schema %s: {{err}}", schemaName), err)
		}
	}

	// Revoke the owner privileges if we had to grant it.
	if ownerGranted {
		err = revokeRoleMembership(txn, schemaOwner, currentUser)
		if err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error revoking owner membership for schema %s: {{err}}", schemaName), err)
		}
	}

	if err := txn.Commit(); err != nil {
		return errwrap.Wrapf("Error committing schema: {{err}}", err)
	}

	d.SetId(generateSchemaID(d, c))

	return resourcePostgreSQLSchemaReadImpl(d, c)
}

func resourcePostgreSQLSchemaDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	database := getDatabase(d, c)

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)

	dropMode := "RESTRICT"
	if d.Get(schemaDropCascade).(bool) {
		dropMode = "CASCADE"
	}

	// Needed in order to drop the schema if the connection user is not a
	// superuser
	currentUser := c.config.getDatabaseUsername()
	ownerGranted := false
	if schemaOwner != "" {
		ownerGranted, err = grantRoleMembership(txn, schemaOwner, currentUser)
		if err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error granting owner membership for schema %s: {{err}}", schemaName), err)
		}
	}

	sql := fmt.Sprintf("DROP SCHEMA %s %s", pq.QuoteIdentifier(schemaName), dropMode)
	if _, err = txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error deleting schema: {{err}}", err)
	}

	// Revoke the owner privileges if we had to grant it.
	if ownerGranted {
		err = revokeRoleMembership(txn, schemaOwner, currentUser)
		if err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error revoking owner membership for schema %s: {{err}}", schemaName), err)
		}
	}

	if err := txn.Commit(); err != nil {
		return errwrap.Wrapf("Error committing schema: {{err}}", err)
	}

	d.SetId("")

	return nil
}

func resourcePostgreSQLSchemaExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*Client)

	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	database, schemaName, err := getDBSchemaName(d, c)
	if err != nil {
		return false, err
	}

	// Check if the database exists
	exists, err := dbExists(c.DB(), database)
	if err != nil || !exists {
		return false, err
	}

	txn, err := startTransaction(c, database)
	if err != nil {
		return false, err
	}
	defer deferredRollback(txn)

	err = txn.QueryRow("SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE n.nspname=$1", schemaName).Scan(&schemaName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errwrap.Wrapf("Error reading schema: {{err}}", err)
	}

	return true, nil
}

func resourcePostgreSQLSchemaRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	return resourcePostgreSQLSchemaReadImpl(d, c)
}

func resourcePostgreSQLSchemaReadImpl(d *schema.ResourceData, c *Client) error {
	database, schemaName, err := getDBSchemaName(d, c)
	if err != nil {
		return err
	}

	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	var schemaOwner string
	var schemaACLs []string
	err = txn.QueryRow("SELECT pg_catalog.pg_get_userbyid(n.nspowner), COALESCE(n.nspacl, '{}'::aclitem[])::TEXT[] FROM pg_catalog.pg_namespace n WHERE n.nspname=$1", schemaName).Scan(&schemaOwner, pq.Array(&schemaACLs))
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL schema (%s) not found in database %s", schemaName, database)
		d.SetId("")
		return nil
	case err != nil:
		return errwrap.Wrapf("Error reading schema: {{err}}", err)
	default:
		type RoleKey string
		schemaPolicies := make(map[RoleKey]acl.Schema, len(schemaACLs))
		for _, aclStr := range schemaACLs {
			aclItem, err := acl.Parse(aclStr)
			if err != nil {
				return errwrap.Wrapf("Error parsing aclitem: {{err}}", err)
			}

			schemaACL, err := acl.NewSchema(aclItem)
			if err != nil {
				return errwrap.Wrapf("invalid perms for schema: {{err}}", err)
			}

			roleKey := RoleKey(strings.ToLower(schemaACL.Role))
			var mergedPolicy acl.Schema
			if existingRolePolicy, ok := schemaPolicies[roleKey]; ok {
				mergedPolicy = existingRolePolicy.Merge(schemaACL)
			} else {
				mergedPolicy = schemaACL
			}
			schemaPolicies[roleKey] = mergedPolicy
		}

		d.Set(schemaNameAttr, schemaName)
		d.Set(schemaOwnerAttr, schemaOwner)
		d.Set(schemaDatabaseAttr, database)
		d.SetId(generateSchemaID(d, c))

		return nil
	}
}

func resourcePostgreSQLSchemaUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	database := getDatabase(d, c)

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	// Needed in order to set policies against the schema if the connection user is not a
	// superuser
	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)
	currentUser := c.config.getDatabaseUsername()
	ownerGranted := false
	if schemaOwner != "" {
		ownerGranted, err = grantRoleMembership(txn, schemaOwner, currentUser)
		if err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error granting owner membership for schema %s: {{err}}", schemaName), err)
		}
	}

	if err := setSchemaName(txn, d, c); err != nil {
		return err
	}

	if err := setSchemaOwner(txn, d); err != nil {
		return err
	}

	if err := setSchemaPolicy(txn, d); err != nil {
		return err
	}

	// Revoke the owner privileges if we had to grant it.
	if ownerGranted {
		err = revokeRoleMembership(txn, schemaOwner, currentUser)
		if err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error revoking owner membership for schema %s: {{err}}", schemaName), err)
		}
	}

	if err := txn.Commit(); err != nil {
		return errwrap.Wrapf("Error committing schema: {{err}}", err)
	}

	return resourcePostgreSQLSchemaReadImpl(d, c)
}

func setSchemaName(txn *sql.Tx, d *schema.ResourceData, c *Client) error {
	if !d.HasChange(schemaNameAttr) {
		return nil
	}

	oraw, nraw := d.GetChange(schemaNameAttr)
	o := oraw.(string)
	n := nraw.(string)
	if n == "" {
		return errors.New("Error setting schema name to an empty string")
	}

	sql := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s", pq.QuoteIdentifier(o), pq.QuoteIdentifier(n))
	if _, err := txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating schema NAME: {{err}}", err)
	}
	d.SetId(generateSchemaID(d, c))

	return nil
}

func setSchemaOwner(txn *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(schemaOwnerAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)

	if schemaOwner == "" {
		return errors.New("Error setting schema owner to an empty string")
	}

	sql := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(schemaOwner))
	if _, err := txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating schema OWNER: {{err}}", err)
	}

	return nil
}

func setSchemaPolicy(txn *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(schemaPolicyAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)

	oraw, nraw := d.GetChange(schemaPolicyAttr)
	oldList := oraw.(*schema.Set).List()
	newList := nraw.(*schema.Set).List()
	queries := make([]string, 0, len(oldList)+len(newList))
	dropped, added, updated, _ := schemaChangedPolicies(oldList, newList)

	for _, p := range dropped {
		pMap := p.(map[string]interface{})
		rolePolicy := schemaPolicyToACL(pMap)

		// The PUBLIC role can not be DROP'ed, therefore we do not need
		// to prevent revoking against it not existing.
		if rolePolicy.Role != "" {
			var foundUser bool
			err := txn.QueryRow(`SELECT TRUE FROM pg_catalog.pg_roles WHERE rolname = $1`, rolePolicy.Role).Scan(&foundUser)
			switch {
			case err == sql.ErrNoRows:
				// Don't execute this role's REVOKEs because the role
				// was dropped first and therefore doesn't exist.
			case err != nil:
				return errwrap.Wrapf("Error reading schema: {{err}}", err)
			default:
				queries = append(queries, rolePolicy.Revokes(schemaName)...)
			}
		}
	}

	for _, p := range added {
		pMap := p.(map[string]interface{})
		rolePolicy := schemaPolicyToACL(pMap)
		queries = append(queries, rolePolicy.Grants(schemaName)...)
	}

	for _, p := range updated {
		policies := p.([]interface{})
		if len(policies) != 2 {
			panic("expected 2 policies, old and new")
		}

		{
			oldPolicies := policies[0].(map[string]interface{})
			rolePolicy := schemaPolicyToACL(oldPolicies)
			queries = append(queries, rolePolicy.Revokes(schemaName)...)
		}

		{
			newPolicies := policies[1].(map[string]interface{})
			rolePolicy := schemaPolicyToACL(newPolicies)
			queries = append(queries, rolePolicy.Grants(schemaName)...)
		}
	}

	for _, query := range queries {
		if _, err := txn.Exec(query); err != nil {
			return errwrap.Wrapf("Error updating schema DCL: {{err}}", err)
		}
	}

	return nil
}

// schemaChangedPolicies walks old and new to create a set of queries that can
// be executed to enact each type of state change (roles that have been dropped
// from the policy, added to a policy, have updated privilges, or are
// unchanged).
func schemaChangedPolicies(old, new []interface{}) (dropped, added, update, unchanged map[string]interface{}) {
	type RoleKey string
	oldLookupMap := make(map[RoleKey]interface{}, len(old))
	for idx := range old {
		v := old[idx]
		schemaPolicy := v.(map[string]interface{})
		if roleRaw, ok := schemaPolicy[schemaPolicyRoleAttr]; ok {
			role := roleRaw.(string)
			roleKey := strings.ToLower(role)
			oldLookupMap[RoleKey(roleKey)] = schemaPolicy
		}
	}

	newLookupMap := make(map[RoleKey]interface{}, len(new))
	for idx := range new {
		v := new[idx]
		schemaPolicy := v.(map[string]interface{})
		if roleRaw, ok := schemaPolicy[schemaPolicyRoleAttr]; ok {
			role := roleRaw.(string)
			roleKey := strings.ToLower(role)
			newLookupMap[RoleKey(roleKey)] = schemaPolicy
		}
	}

	droppedRoles := make(map[string]interface{}, len(old))
	for kOld, vOld := range oldLookupMap {
		if _, ok := newLookupMap[kOld]; !ok {
			droppedRoles[string(kOld)] = vOld
		}
	}

	addedRoles := make(map[string]interface{}, len(new))
	for kNew, vNew := range newLookupMap {
		if _, ok := oldLookupMap[kNew]; !ok {
			addedRoles[string(kNew)] = vNew
		}
	}

	updatedRoles := make(map[string]interface{}, len(new))
	unchangedRoles := make(map[string]interface{}, len(new))
	for kOld, vOld := range oldLookupMap {
		if vNew, ok := newLookupMap[kOld]; ok {
			if reflect.DeepEqual(vOld, vNew) {
				unchangedRoles[string(kOld)] = vOld
			} else {
				updatedRoles[string(kOld)] = []interface{}{vOld, vNew}
			}
		}
	}

	return droppedRoles, addedRoles, updatedRoles, unchangedRoles
}

func schemaPolicyToHCL(s *acl.Schema) map[string]interface{} {
	return map[string]interface{}{
		schemaPolicyRoleAttr:            s.Role,
		schemaPolicyCreateAttr:          s.GetPrivilege(acl.Create),
		schemaPolicyCreateWithGrantAttr: s.GetGrantOption(acl.Create),
		schemaPolicyUsageAttr:           s.GetPrivilege(acl.Usage),
		schemaPolicyUsageWithGrantAttr:  s.GetGrantOption(acl.Usage),
	}
}

func schemaPolicyToACL(policyMap map[string]interface{}) acl.Schema {
	var rolePolicy acl.Schema

	if policyMap[schemaPolicyCreateAttr].(bool) {
		rolePolicy.Privileges |= acl.Create
	}

	if policyMap[schemaPolicyCreateWithGrantAttr].(bool) {
		rolePolicy.Privileges |= acl.Create
		rolePolicy.GrantOptions |= acl.Create
	}

	if policyMap[schemaPolicyUsageAttr].(bool) {
		rolePolicy.Privileges |= acl.Usage
	}

	if policyMap[schemaPolicyUsageWithGrantAttr].(bool) {
		rolePolicy.Privileges |= acl.Usage
		rolePolicy.GrantOptions |= acl.Usage
	}

	if roleRaw, ok := policyMap[schemaPolicyRoleAttr]; ok {
		rolePolicy.Role = roleRaw.(string)
	}

	return rolePolicy
}

func generateSchemaID(d *schema.ResourceData, c *Client) string {
	SchemaID := strings.Join([]string{
		getDatabase(d, c),
		d.Get(schemaNameAttr).(string),
	}, ".")

	return SchemaID
}

func getSchemaNameFromID(ID string) string {
	splitted := strings.Split(ID, ".")
	return splitted[0]
}

func getDBSchemaName(d *schema.ResourceData, client *Client) (string, string, error) {
	database := getDatabase(d, client)
	schemaName := d.Get(schemaNameAttr).(string)

	// When importing, we have to parse the ID to find schema and database names.
	if schemaName == "" {
		parsed := strings.Split(d.Id(), ".")
		if len(parsed) != 2 {
			return "", "", fmt.Errorf("schema ID %s has not the expected format 'database.schema': %v", d.Id(), parsed)
		}
		database = parsed[0]
		schemaName = parsed[1]
	}
	return database, schemaName, nil
}
