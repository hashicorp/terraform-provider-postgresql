#!/bin/bash
go clean -testcache
TF_ACC=1 go test ./postgresql -v -timeout 120m

# for a single test comment the previous line and uncomment the next line
#TF_LOG=INFO TF_ACC=1 go test -v ./postgresql -run ^TestAccPostgresqlRole_Basic$ -timeout 360s