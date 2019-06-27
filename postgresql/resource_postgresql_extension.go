package postgresql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/lib/pq"
)

const (
	extNameAttr     = "name"
	extSchemaAttr   = "schema"
	extVersionAttr  = "version"
	extDatabaseAttr = "database"
)

func resourcePostgreSQLExtension() *schema.Resource {
	return &schema.Resource{
		Create: resourcePostgreSQLExtensionCreate,
		Read:   resourcePostgreSQLExtensionRead,
		Update: resourcePostgreSQLExtensionUpdate,
		Delete: resourcePostgreSQLExtensionDelete,
		Exists: resourcePostgreSQLExtensionExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			extNameAttr: {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			extSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Sets the schema of an extension",
			},
			extVersionAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Sets the version number of the extension",
			},
			extDatabaseAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Sets the database to add the extension to",
			},
		},
	}
}

func resourcePostgreSQLExtensionCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	if !c.featureSupported(featureExtension) {
		return fmt.Errorf(
			"postgresql_extension resource is not supported for this Postgres version (%s)",
			c.version,
		)
	}

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	extName := d.Get(extNameAttr).(string)

	b := bytes.NewBufferString("CREATE EXTENSION IF NOT EXISTS ")
	fmt.Fprint(b, pq.QuoteIdentifier(extName))

	if v, ok := d.GetOk(extSchemaAttr); ok {
		fmt.Fprint(b, " SCHEMA ", pq.QuoteIdentifier(v.(string)))
	}

	if v, ok := d.GetOk(extVersionAttr); ok {
		fmt.Fprint(b, " VERSION ", pq.QuoteIdentifier(v.(string)))
	}

	database := getDatabaseForExtension(d, c)

	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	sql := b.String()
	if _, err := txn.Exec(sql); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return errwrap.Wrapf("Error creating extension: {{err}}", err)
	}

	d.SetId(generateExtensionID(d, c))

	return resourcePostgreSQLExtensionReadImpl(d, meta)
}

func resourcePostgreSQLExtensionExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*Client)

	if !c.featureSupported(featureExtension) {
		return false, fmt.Errorf(
			"postgresql_extension resource is not supported for this Postgres version (%s)",
			c.version,
		)
	}

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	var extensionName string

	database := getDatabaseForExtension(d, c)
	txn, err := startTransaction(c, database)
	if err != nil {
		return false, err
	}
	defer deferredRollback(txn)

	query := "SELECT extname FROM pg_catalog.pg_extension WHERE extname = $1"
	err = txn.QueryRow(query, d.Get(extNameAttr).(string)).Scan(&extensionName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourcePostgreSQLExtensionRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	if !c.featureSupported(featureExtension) {
		return fmt.Errorf(
			"postgresql_extension resource is not supported for this Postgres version (%s)",
			c.version,
		)
	}

	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	return resourcePostgreSQLExtensionReadImpl(d, meta)
}

func resourcePostgreSQLExtensionReadImpl(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	database := getDatabaseForExtension(d, c)
	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	var extName, extSchema, extVersion string
	query := `SELECT e.extname, n.nspname, e.extversion ` +
		`FROM pg_catalog.pg_extension e, pg_catalog.pg_namespace n ` +
		`WHERE n.oid = e.extnamespace AND e.extname = $1`
	err = txn.QueryRow(query, d.Get(extNameAttr).(string)).Scan(&extName, &extSchema, &extVersion)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL extension (%s) not found", d.Get(extNameAttr).(string))
		d.SetId("")
		return nil
	case err != nil:
		return errwrap.Wrapf("Error reading extension: {{err}}", err)
	}

	d.Set(extNameAttr, extName)
	d.Set(extSchemaAttr, extSchema)
	d.Set(extVersionAttr, extVersion)
	d.Set(extDatabaseAttr, database)
	d.SetId(generateExtensionID(d, meta.(*Client)))

	return nil
}

func resourcePostgreSQLExtensionDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	if !c.featureSupported(featureExtension) {
		return fmt.Errorf(
			"postgresql_extension resource is not supported for this Postgres version (%s)",
			c.version,
		)
	}

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	extName := d.Get(extNameAttr).(string)

	database := getDatabaseForExtension(d, c)
	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	sql := fmt.Sprintf("DROP EXTENSION %s", pq.QuoteIdentifier(extName))
	if _, err := txn.Exec(sql); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return errwrap.Wrapf("Error deleting extension: {{err}}", err)
	}

	d.SetId("")

	return nil
}

func resourcePostgreSQLExtensionUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	if !c.featureSupported(featureExtension) {
		return fmt.Errorf(
			"postgresql_extension resource is not supported for this Postgres version (%s)",
			c.version,
		)
	}

	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	database := getDatabaseForExtension(d, c)
	txn, err := startTransaction(c, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	// Can't rename a schema

	if err := setExtSchema(txn, d); err != nil {
		return err
	}

	if err := setExtVersion(txn, d); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return errwrap.Wrapf("Error updating extension: {{err}}", err)
	}

	return resourcePostgreSQLExtensionReadImpl(d, meta)
}

func setExtSchema(txn *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(extSchemaAttr) {
		return nil
	}

	extName := d.Get(extNameAttr).(string)
	_, nraw := d.GetChange(extSchemaAttr)
	n := nraw.(string)
	if n == "" {
		return errors.New("Error setting extension name to an empty string")
	}

	sql := fmt.Sprintf("ALTER EXTENSION %s SET SCHEMA %s",
		pq.QuoteIdentifier(extName), pq.QuoteIdentifier(n))
	if _, err := txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating extension SCHEMA: {{err}}", err)
	}

	return nil
}

func setExtVersion(txn *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(extVersionAttr) {
		return nil
	}

	extName := d.Get(extNameAttr).(string)

	b := bytes.NewBufferString("ALTER EXTENSION ")
	fmt.Fprintf(b, "%s UPDATE", pq.QuoteIdentifier(extName))

	_, nraw := d.GetChange(extVersionAttr)
	n := nraw.(string)
	if n != "" {
		fmt.Fprintf(b, " TO %s", pq.QuoteIdentifier(n))
	}

	sql := b.String()
	if _, err := txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating extension version: {{err}}", err)
	}

	return nil
}

func getDatabaseForExtension(d *schema.ResourceData, client *Client) string {
	database := client.databaseName
	if v, ok := d.GetOk(extDatabaseAttr); ok {
		database = v.(string)
	}

	return database
}

func generateExtensionID(d *schema.ResourceData, client *Client) string {
	return strings.Join([]string{
		d.Get(extNameAttr).(string), getDatabaseForExtension(d, client),
	}, ".")
}

func getExtensionNameFromID(ID string) string {
	splitted := strings.Split(ID, ".")
	return strings.Join(splitted[:len(splitted)-1], "")
}
