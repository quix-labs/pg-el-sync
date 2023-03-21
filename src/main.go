package main

import (
	"go_pg_es_sync/internals"
	"log"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatalln("You need to specify action [ listen | index ]")
	}

	config := &internals.Config{}
	err := config.LoadFromYaml("/app/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	pgSync := &internals.PgSync{}
	err = pgSync.Init(config)
	if err != nil {
		log.Fatal(err)
	}
	defer pgSync.Terminate()

	switch args[0] {
	case "listen":
		//sigs := make(chan os.Signal, 1)
		//signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		go pgSync.Start()
		select {}
		//<-sigs
	case "index":
		pgSync.FullReindex()
	case "stats":
		log.Fatalln("Not implemented")
	default:
		log.Fatalf("Undefined action %s\n", args[0])
	}
}
