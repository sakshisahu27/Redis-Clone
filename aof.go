package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

type Aof struct {
	w *Writer
	f *os.File
	conf *Config
}

func NewAof(conf *Config) *Aof {
	aof := Aof{conf: conf}

	fp := path.Join(aof.conf.dir, aof.conf.aofFn)
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("cannot open: ", fp)
		return &aof
	}
	aof.w = NewWriter(f)
	aof.f = f

	return &aof
}

func (aof *Aof) Sync() {
	for {
		v := Value{}
		err := v.readArray(aof.f)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("unexpected error while reading AOF records: ", err)
			break
		}

		blankState := NewAppState(&Config{})
		blankClient := Client{}
		set(&blankClient, &v, blankState)
	}
}

// Write all SET commands to file
func (aof *Aof) Rewrite(cp map[string]*Key) {
	// reroute future AOF records to buffer
	var b bytes.Buffer
	aof.w = NewWriter(&b)

	//clear the file contents
	if err := aof.f.Truncate(0); err != nil {
		log.Println("aof rewrite -  truncate error: ", err)
		return
	}
	if _, err := aof.f.Seek(0, 0); err != nil {
		log.Println("aof rewrite - seek error: ", err)
		return
	}

	fwriter := NewWriter(aof.f)
	for k, v := range cp {
		cmd := Value{typ: BULK, bulk: "SET"}
		key := Value{typ: BULK, bulk: k}
		val := Value{typ: BULK, bulk: v.V}

		arr := Value{typ: ARRAY, array: []Value{cmd, key, val}}
		fwriter.Write(&arr)
	}
	fwriter.Flush()

	if _, err := b.WriteTo(aof.f); err != nil {
		log.Println("aof rewrite - write buffer error: ", err)
		return
	}

	// rewrite future AOF records back to file
	aof.w = NewWriter(aof.f)
}
