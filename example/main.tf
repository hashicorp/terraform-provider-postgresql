provider "google" {
}

provider "random" {}

resource "random_id" "db_name_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "master" {
  name = "master-instance-${random_id.db_name_suffix.hex}"
  database_version = "POSTGRES_11"

  settings {
    tier = "db-f1-micro"
  }
}

resource "random_password" "sql_user_password" {
  length = 20
  special = false
}

resource "google_sql_user" "user" {
  name     = "me"
  instance = google_sql_database_instance.master.name
  host     = "me.com"
  password = random_password.sql_user_password.result
}

provider "postgresql" {
  username = google_sql_user.user.name
  password = google_sql_user.user.password

  database = "postgres"
  gcp_connection_string = google_sql_database_instance.master.connection_name
}

resource "postgresql_database" "my_db" {
  name              = "my_db"
  owner             = "my_role"
  template          = "template0"
  lc_collate        = "C"
  connection_limit  = -1
  allow_connections = true
}

