package main

import (
	"flag"
	"log"

	"github.com/PUSHKAR99055/Distributed-Task-Scheduler/pkg/common"
	"github.com/PUSHKAR99055/Distributed-Task-Scheduler/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "Port on which the Scheduler serves requests.")
)

func main() {
	dbConnectionString := common.GetDBConnectionString()
	schedulerServer := scheduler.NewServer(*schedulerPort, dbConnectionString)
	err := schedulerServer.Start()
	if err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
