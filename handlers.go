package main

import (
	"log"
	"maps"
	"path/filepath"
)

type Handler func(*Client,*Value, *AppState) *Value 

var Handlers = map[string]Handler{
	"COMMAND": command,
	"GET": get,
	"SET": set,
	"DEL": del,
	"EXISTS": exists,
	"KEYS": keys,
	"SAVE": save,
	"BGSAVE": bgSave,
	"FLUSHDB": flushDB,
	"DBSIZE": dbSize,
	"AUTH": auth,
}

var SafeCMDs = []string{
	"COMMAND",
	"AUTH",
}
func handle(c *Client, v *Value, state *AppState) {
	cmd := v.array[0].bulk
	handler, ok := Handlers[cmd]
	w := NewWriter(c.conn)

	if !ok {
		w.Write(&Value{typ: ERROR, err: "ERR invalid command"})
		w.Flush()
		return
	}

	if state.conf.requirePass && !c.authenticated && !contains(SafeCMDs, cmd) {
		w.Write(&Value{typ: ERROR, err: "NOAUTH authentication required"})
		w.Flush()
		return
	}

	reply := handler(c, v, state)
	w.Write(reply)
	w.Flush()
}

func get(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'GET' command"}
	}

	name := args[0].bulk
	DB.mu.RLock()
	val, ok := DB.store[name]
	DB.mu.RUnlock()

	if !ok {
		return &Value{typ: NULL}
	}

	return &Value{typ: BULK, bulk: val}
}

func set(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 2 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'SET' command"}
	}

	key := args[0].bulk
	val := args[1].bulk
	DB.mu.Lock()
	DB.store[key] = val

	if state.conf.aofEnabled {
		log.Println("saving AOF record")
		state.aof.w.Write(v)
		state.aof.w.Flush()  //this

		if state.conf.aofFsync == Always {
			state.aof.w.Flush()
		}
	}

	if len(state.conf.rdb) > 0 {
		IncrRDBTrackers()
	}
	DB.mu.Unlock()
	
	return &Value{typ: STRING, str: "OK"}
}

func del(c *Client,v *Value, state *AppState) *Value {
	args := v.array[1:]
	var n int

	DB.mu.Lock()
	for _, arg := range args {
		_, ok := DB.store[arg.bulk]
			delete(DB.store, arg.bulk)
		if ok {
			n++
		}
	}
	DB.mu.Unlock()
	return &Value{typ: INTEGER, num: n}
}

func exists(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	var n int

	DB.mu.RLock()
	for _, arg := range args {
		_, ok := DB.store[arg.bulk]
		if ok {
			n++
		}
	}
	DB.mu.RUnlock()
	return &Value{typ: INTEGER, num: n}
}

func keys(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) >1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'KEYS' command"}
	}
	
	pattern := args[0].bulk
	
	DB.mu.RLock()
	var matches []string
	for key := range DB.store {
		matched, err := filepath.Match(pattern, key)
		if err != nil {
			log.Printf("error matching keys: (pattern: %s), (key: %s) - %v", pattern, key, err)
			continue
		}
		if matched {
			matches = append(matches, key)
		}
	}
	
	DB.mu.RUnlock()

	reply := Value{typ: ARRAY}

	for _, m := range matches {
		reply.array = append(reply.array, Value{typ: BULK, bulk: m})
	}
	return &reply
}

func save(c *Client, v *Value, state *AppState) *Value {
	SaveRDB(state)
	return &Value{typ: STRING, str: "OK"}
}

func bgSave(c *Client, v *Value, state *AppState) *Value {
	if state.bgSaveRunning {
		return &Value{typ: ERROR, err: "ERR background saving already in progress"}
	}

	cp := make(map[string]string, len(DB.store))
	DB.mu.RLock()
	maps.Copy(cp, DB.store)
	DB.mu.RUnlock()

	state.bgSaveRunning = true
	state.dbCopy = cp

	go func() {
		defer func () {
			state.bgSaveRunning = false
			state.dbCopy = nil
		}()
		SaveRDB(state)
	}()
	
	return &Value{typ: STRING, str: "OK"}
}

func flushDB(c *Client, v *Value, state *AppState) *Value {
	DB.mu.Lock()
	DB.store = map[string]string{}
	DB.mu.Unlock()
	
	return &Value{typ: STRING, str: "OK"}
}

func dbSize(c *Client, v *Value, state *AppState) *Value {
	DB.mu.RLock()
	size := len(DB.store)
	DB.mu.RUnlock()

	return &Value{typ: INTEGER, num: size}
}

func auth(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'AUTH' command"}
	}

	p := args[0].bulk
	if state.conf.password == p {
		c.authenticated = true
		return &Value{typ: STRING, str: "OK"}
	} else {
		c.authenticated = false
		return &Value{typ: ERROR, err: "ERR invalid password"}
	}

}

func command(c *Client, v *Value, state *AppState) *Value {
	return &Value{typ: STRING, str: "OK"}
}
