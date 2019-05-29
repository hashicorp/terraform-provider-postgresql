package postgresql

import (
	"fmt"
	"time"

	"github.com/blang/semver"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

const (
	defaultProviderMaxOpenConnections = uint(4)

	defaultExpectedPostgreSQLVersion = "9.0.0"

	defaultSshUser = "root"

	// defaultSshPort is used if there is no port given
	defaultSshPort = 22

	// defaultSshTimeout is used if there is no timeout given
	defaultSshTimeout = 5 * time.Minute
)

// Provider returns a terraform.ResourceProvider.
func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGHOST", nil),
				Description: "Name of PostgreSQL server address to connect to",
			},
			"port": {
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGPORT", 5432),
				Description: "The PostgreSQL port number to connect to at the server host, or socket file name extension for Unix-domain connections",
			},
			"database": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The name of the database to connect to in order to conenct to (defaults to `postgres`).",
				DefaultFunc: schema.EnvDefaultFunc("PGDATABASE", "postgres"),
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGUSER", "postgres"),
				Description: "PostgreSQL user name to connect as",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGPASSWORD", nil),
				Description: "Password to be used if the PostgreSQL server demands password authentication",
				Sensitive:   true,
			},
			// Connection username can be different than database username with user name maps (e.g.: in Azure)
			// See https://www.postgresql.org/docs/current/auth-username-maps.html
			"database_username": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Database username associated to the connected user (for user name maps)",
			},

			"superuser": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
				Description: "Specify if the user to connect as is a Postgres superuser or not." +
					"If not, some feature might be disabled (e.g.: Refreshing state password from Postgres)",
			},

			"sslmode": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGSSLMODE", nil),
				Description: "This option determines whether or with what priority a secure SSL TCP/IP connection will be negotiated with the PostgreSQL server",
			},
			"ssl_mode": {
				Type:       schema.TypeString,
				Optional:   true,
				Deprecated: "Rename PostgreSQL provider `ssl_mode` attribute to `sslmode`",
			},
			"connect_timeout": {
				Type:         schema.TypeInt,
				Optional:     true,
				DefaultFunc:  schema.EnvDefaultFunc("PGCONNECT_TIMEOUT", 180),
				Description:  "Maximum wait for connection, in seconds. Zero or not specified means wait indefinitely.",
				ValidateFunc: validateConnTimeout,
			},
			"max_connections": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      defaultProviderMaxOpenConnections,
				Description:  "Maximum number of connections to establish to the database. Zero means unlimited.",
				ValidateFunc: validateMaxConnections,
			},
			"expected_version": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      defaultExpectedPostgreSQLVersion,
				Description:  "Specify the expected version of PostgreSQL.",
				ValidateFunc: validateExpectedVersion,
			},
			"connection": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "", // TODO
				MaxItems:    1,
				// TODO validate the connection configuration
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"bastion_user": {
							Type:        schema.TypeString,
							Optional:    true,
							Default:     defaultSshUser,
							Description: "The user for the connection to the bastion host. Defaults to the value of the user field.",
						},
						"bastion_password": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "",
						},
						"bastion_private_key": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "The contents of an SSH key file to use for the bastion host.",
						},
						"bastion_host": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "Setting this enables the bastion Host connection. This host will be connected to first, and then the host connection will be made from there.",
						},
						"bastion_host_key": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "The public key from the remote host or the signing CA, used to verify the host connection.",
						},
						"bastion_port": {
							Type:        schema.TypeInt,
							Optional:    true,
							Default:     defaultSshPort,
							Description: "The port to use connect to the bastion host. Defaults to the value of the port field.",
						},
						"timeout": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "The timeout to wait for the connection to become available. This defaults to 5 minutes.",
						},
						"agent": {
							Type:        schema.TypeBool,
							Optional:    true,
							Default:     true,
							Description: "Set to false to disable using ssh-agent to authenticate.",
						},
					},
				},
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"postgresql_database":           resourcePostgreSQLDatabase(),
			"postgresql_default_privileges": resourcePostgreSQLDefaultPrivileges(),
			"postgresql_extension":          resourcePostgreSQLExtension(),
			"postgresql_grant":              resourcePostgreSQLGrant(),
			"postgresql_schema":             resourcePostgreSQLSchema(),
			"postgresql_role":               resourcePostgreSQLRole(),
		},

		ConfigureFunc: providerConfigure,
	}
}

func validateConnTimeout(v interface{}, key string) (warnings []string, errors []error) {
	value := v.(int)
	if value < 0 {
		errors = append(errors, fmt.Errorf("%s can not be less than 0", key))
	}
	return
}

func validateExpectedVersion(v interface{}, key string) (warnings []string, errors []error) {
	if _, err := semver.Parse(v.(string)); err != nil {
		errors = append(errors, fmt.Errorf("invalid version (%q): %v", v.(string), err))
	}
	return
}

func validateMaxConnections(v interface{}, key string) (warnings []string, errors []error) {
	value := v.(int)
	if value < 1 {
		errors = append(errors, fmt.Errorf("%s can not be less than 1", key))
	}
	return
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	var sslMode string
	if sslModeRaw, ok := d.GetOk("sslmode"); ok {
		sslMode = sslModeRaw.(string)
	} else {
		sslModeDeprecated := d.Get("ssl_mode").(string)
		if sslModeDeprecated != "" {
			sslMode = sslModeDeprecated
		}
	}
	versionStr := d.Get("expected_version").(string)
	version, _ := semver.Parse(versionStr)

	config := Config{
		Host:              d.Get("host").(string),
		Port:              d.Get("port").(int),
		Username:          d.Get("username").(string),
		Password:          d.Get("password").(string),
		DatabaseUsername:  d.Get("database_username").(string),
		Superuser:         d.Get("superuser").(bool),
		SSLMode:           sslMode,
		ApplicationName:   tfAppName(),
		ConnectTimeoutSec: d.Get("connect_timeout").(int),
		MaxConns:          d.Get("max_connections").(int),
		ExpectedVersion:   version,
	}

	// TODO configure using a hashset?

	if conns, ok := d.Get("connection").([]interface{}); ok && len(conns) == 1 {
		conn := conns[0].(map[string]interface{})

		config.SshUser = conn["bastion_user"].(string)
		config.SshPassword = conn["bastion_password"].(string)
		config.SshPrivateKey = conn["bastion_private_key"].(string)
		config.SshHost = conn["bastion_host"].(string)
		config.SshHostKey = conn["bastion_host_key"].(string)
		config.SshPort = conn["bastion_port"].(int)

		// TODO allow configure timeout (with correct parsing)
		//config.Timeout = conn["timeout"].(int)

		config.SshAgent = conn["agent"].(bool)

		config.Ssh = config.SshHost != ""
	}

	client, err := config.NewClient(d.Get("database").(string))
	if err != nil {
		return nil, errwrap.Wrapf("Error initializing PostgreSQL client: {{err}}", err)
	}

	return client, nil
}

func tfAppName() string {
	return fmt.Sprintf("Terraform v%s", terraform.VersionString())
}
