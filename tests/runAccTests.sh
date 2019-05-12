#!/bin/bash
set -e

log() {
  echo "####################"
  echo "## ->  $1 "
  echo "####################"
}

setup() {
    export TF_ACC=true
    export PGHOST=localhost
    export PGPORT=25432
    export PGUSER=postgres
    export PGPASSWORD=testpwd
    export PGSSLMODE=disable

    docker-compose -f "$(pwd)"/tests/docker-compose.yml up -d
    sh "$(pwd)"/tests/wait-postgres-docker.sh "$(pwd)"/tests/docker-compose.yml
}

run() {
  go clean -testcache
  TF_ACC=1 go test ./postgresql -v -timeout 120m
  
  # for a single test comment the previous line and uncomment the next line
  #TF_LOG=INFO TF_ACC=1 go test -v ./postgresql -run ^TestAccPostgresqlRole_Basic$ -timeout 360s
  
  # keep the return value for the scripts to fail and clean properly
  return $?
}

cleanup() {
  docker-compose -f "$(pwd)"/tests/docker-compose.yml down
  unset TF_ACC PGHOST PGPORT PGUSER PGPASSWORD PGSSLMODE
}

## main
log "setup" && setup 
log "run" && run || (log "cleanup" && cleanup && exit 1)
log "cleanup" && cleanup
