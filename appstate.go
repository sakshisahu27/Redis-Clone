package main

import "time"

type AppState struct {
	conf          *Config
	aof           *Aof
	bgSaveRunning bool
	dbCopy        map[string]*Item
	tx            *Transaction
	monitors	  []*Client
}

func NewAppState(conf *Config) *AppState {
	state := AppState{conf: conf}

	if conf.aofEnabled {
		state.aof = NewAof(conf)

		if conf.aofFsync == EverySec {
			go func() {
				t := time.NewTicker(time.Second)
				defer t.Stop()
				for range t.C {
					state.aof.w.Flush()
				}
			}()
		}
	}

	return &state
}
