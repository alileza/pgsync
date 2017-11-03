# PGSync

PostgreSQL database sync/replication

**NOTES** : It's currently only support 1 primaryKey, must be `int` and Auto-Increment.

# Getting Started
## Installation
```
go get -u github.com/alileza/pgsync
```
or 
Download the latest release [http://github.com/alileza/pgsync/releases/latest](http://github.com/alileza/pgsync/releases/latest)

## Usage
```
usage: pgsync --src=SRC --dest=DEST [<flags>]

Flags:
      --help              Show context-sensitive help (also try --help-long and --help-man).
  -v, --verbose           Verbose mode.
  -i, --src=SRC           database source datasource name
  -d, --dest=DEST         database destination datasource name
  -x, --exclude=EXCLUDE   exclude some tables
  -s, --only=ONLY         select specific tables
  -t, --sync_interval=1m
  -p, --prometheus_port=PROMETHEUS_PORT

  -c, --chunk=CHUNK       fetch query amount
  ```
