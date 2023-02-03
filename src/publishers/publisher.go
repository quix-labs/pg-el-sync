package publishers

import (
	"github.com/rs/zerolog"
	"os"
)

type Publisher struct {
	Logger zerolog.Logger
}

// Global method

func (p *Publisher) InternalInit(name string) {
	p.Logger = zerolog.New(os.Stdout).
		With().Caller().Stack().Timestamp().
		Str("service", "publisher").Str("serviceName", name).
		Logger()
}
func (p *Publisher) InternalTerminate() {}
