package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	dir string
	rdb []RDBSnapshot
	rdbFn string
	aofEnabled bool
	aofFn string
	aofFsync FsyncMode
	requirePass bool
	password string
}

func NewConfig() *Config {
	return &Config{}
}

type RDBSnapshot struct {
	Secs int
	KeysChanged int
}

type FsyncMode string

const (
	Always FsyncMode = "always"
	EverySec FsyncMode = "everysec"
	No FsyncMode = "no"
)

func readConf(fn string) *Config {
	conf := NewConfig()

	f, err := os.Open(fn)
	if err != nil {
		fmt.Printf("cannot read %s - using default config\n", fn)
		return conf
	}
	defer f.Close()

	s := bufio.NewScanner(f)

	for s.Scan() {
		l := s.Text()
		parseLine(l, conf)
	}
	if err := s.Err(); err != nil {
		fmt.Println("error reading config file: ", err)
		return conf
	}

	if conf.dir != "" {
		os.MkdirAll(conf.dir, 0755)
	}
	return conf
}

func parseLine(line string, conf *Config) {
	args := strings.Split(line, " ")
	cmd := args[0]

	switch cmd {
	case "save":
		secs, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("invalid secs")
			return
		}

		KeysChanged, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Println("invalid keys")
			return
		}

		snapshot := RDBSnapshot{
			Secs: secs, 
			KeysChanged: KeysChanged,
		}
		conf.rdb = append(conf.rdb, snapshot)

	case "dbfilename":
		conf.rdbFn = args[1]

	case "appendfilename":
		conf.aofFn = args[1]

	case "appendfsync":
		conf.aofFsync = FsyncMode(args[1])

	case "appendonly":
		if args[1] == "yes" {
			conf.aofEnabled = true
		} else {
			conf.aofEnabled = false
		}

	case "dir":
		conf.dir = args[1]
	case "requirepass":
		conf.requirePass = true
		conf.password = args[1]
	}
}