package main

import (
	"github.com/quix-labs/pg-el-sync/internals"
	"log"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatalln("You need to specify action [ listen | index ]")
	}

	config := &internals.Config{}

	configFile := os.Getenv("CONFIG_FILE")
	if configFile == "" {
		configFile = "/app/config.yaml"
	}
	err := config.LoadFromYaml(configFile)
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
