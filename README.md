# Redis-Clone (Go)

A lightweight **Redis-like in-memory key-value store** written in **Go**, built to understand core Redis concepts such as:

- RESP-style command handling over TCP (`:6379`)
- Basic string commands (`GET`, `SET`, `DEL`, `EXISTS`, `KEYS`)
- Key expiry (`EXPIRE`, `TTL`)
- Simple transactions (`MULTI`, `EXEC`, `DISCARD`)
- Persistence
  - **AOF** (Append Only File) with optional background rewrite
  - **RDB** snapshots (periodic + “keys changed” threshold)
- Authentication (`AUTH`) via `requirepass`
- Basic memory limit + eviction policies (e.g. `allkeys-lfu`)
- Monitoring + stats (`MONITOR`, `INFO`)

> Note: This is a learning project and **not a production-ready Redis replacement**.

---

## Features

### Supported commands
- `COMMAND`
- `AUTH <password>`
- `GET <key>`
- `SET <key> <value>`
- `DEL <key> [key ...]`
- `EXISTS <key> [key ...]`
- `KEYS <pattern>`
- `ITEMS <pattern>` (project-specific alias similar to `KEYS`)
- `EXPIRE <key> <seconds>`
- `TTL <key>`
- `DBSIZE`
- `FLUSHDB`
- `SAVE` / `BGSAVE`
- `BGREWRITEAOF`
- `MULTI` / `EXEC` / `DISCARD`
- `MONITOR`
- `INFO`

---

## Project structure (high level)

- `main.go` – TCP server (`net.Listen`) and connection loop
- `handlers.go` – command router + handlers for supported commands
- `db.go` – in-memory store + eviction + expiry checks
- `aof.go` – AOF persistence + rewrite support
- `rdb.go` – RDB snapshot save/load + trackers
- `conf.go` – reads a `redis.conf`-style config file
- `info.go` – builds the output for the `INFO` command

---

## Getting started

### Prerequisites
- Go installed (any recent version that supports your `go.mod`)
- Port **6379** available on your machine

### Clone & run
```bash
git clone https://github.com/sakshisahu27/Redis-Clone.git
cd Redis-Clone
go run .
```

By default the server listens on:
- `localhost:6379`

---

## Configuration (`redis.conf`)

The server reads `./redis.conf` on startup.

Example (from this repo):
```conf
dir ./data

# AOF
appendonly yes
appendfilename backup.aof
appendfsync everysec

# RDB
save 5 3
dbfilename backup.rdb

# AUTH
requirepass dolphins

# MEMORY
maxmemory 256
maxmemory-policy allkeys-lfu
maxmemory-samples 50
```

### Notes
- `dir` is the persistence directory (created if it doesn’t exist).
- `appendonly yes` enables AOF.
- `save <seconds> <keys>` configures RDB snapshots (save if at least `<keys>` keys changed within `<seconds>`).
- `requirepass` enables authentication: you must run `AUTH` before most commands.
- `maxmemory` enables memory limiting; eviction policy depends on `maxmemory-policy`.

---

## Using the server

### Connect with `redis-cli`
If you have Redis installed locally:
```bash
redis-cli -p 6379
```

If `requirepass` is set:
```bash
AUTH dolphins
```

### Example session
```txt
SET name sakshi
GET name
EXPIRE name 10
TTL name
KEYS *
DBSIZE
INFO
```

### Monitoring
In one terminal:
```bash
redis-cli -p 6379
AUTH dolphins
MONITOR
```

In another terminal, run commands and you should see them logged to the monitor connection.

---

## Persistence

### AOF (Append Only File)
- When enabled, `SET` operations are appended to the AOF file.
- On startup, AOF can be replayed to rebuild state.
- `BGREWRITEAOF` rewrites the AOF file in the background by compacting it into a sequence of `SET` commands for the current dataset.

### RDB snapshots
- `SAVE` performs a synchronous snapshot.
- `BGSAVE` performs snapshotting using a copy of the in-memory map.

---

## Limitations / differences vs Redis

- Focuses on a subset of Redis commands and behaviors.
- Data types are primarily simple string values (not full Redis data structures).
- Concurrency and correctness are designed for learning, not heavy production workloads.
- Protocol compatibility aims to be Redis-like, but may not match Redis edge cases.
