package subscribers

import (
	"github.com/rs/zerolog"
	"os"
)

type Subscriber struct {
	Channel *chan *interface{}
	Logger  zerolog.Logger
}

// Global method

func (s *Subscriber) InternalInit(eventChannel *chan *interface{}, name string) {
	s.Channel = eventChannel
	s.Logger = zerolog.New(os.Stdout).
		With().Caller().Stack().Timestamp().
		Str("service", "subscriber").Str("serviceName", name).
		Logger()
}

func (s *Subscriber) DispatchEvent(event *interface{}) {
	*s.Channel <- event
}

func (s *Subscriber) InternalTerminate() {}
