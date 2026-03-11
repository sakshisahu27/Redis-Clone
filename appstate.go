package main

import "time"

type RDBstats struct {
	rdb_last_save_ts int64
	rdb_saves        int
}

type AOFstats struct {
	aof_rewrites int
}

type GeneralStats struct {
	total_connections_received int
	total_commands_processed   int
	expired_keys               int
	evicted_keys               int
}

type AppState struct {
	conf              *Config
	aof               *Aof
	bgSaveRunning     bool
	aofRewriteRunning bool
	dbCopy            map[string]*Item
	tx                *Transaction
	monitors          []*Client
	serverStart       time.Time
	clientCount       int
	peakMem           int64
	info              *Info
	rdbstats          RDBstats
	aofstats          AOFstats
	generalstats      GeneralStats
}

func NewAppState(conf *Config) *AppState {
	state := AppState{
		conf:         conf,
		serverStart:  time.Now(),
		info:         NewInfo(),
		rdbstats:     RDBstats{},
		aofstats:     AOFstats{},
		generalstats: GeneralStats{},
	}

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
