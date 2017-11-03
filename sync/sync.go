package sync

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/alileza/pgsync/syncmap"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pgsyncLastTableSync = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pgsync_last_table_sync_timestamp_seconds",
			Help: "Timestamp of the last successful table sync.",
		},
		[]string{"table_name"},
	)
)

func init() {
	prometheus.MustRegister(pgsyncLastTableSync)
}

type Sync struct {
	src  *sqlx.DB
	dest *sqlx.DB

	options       *Options
	listenErrChan chan error
	storage       *syncmap.SyncMap
}

type Options struct {
	Chunk         int
	SyncInterval  time.Duration
	IncludeTables []string
	ExcludeTables []string
}

var getTablesQuery = `
    SELECT
      table_name
    FROM
      information_schema.tables
    WHERE
      table_schema='public'
    `

func NewSync(src, dest *sql.DB, o ...*Options) *Sync {
	s := &Sync{
		src:           sqlx.NewDb(src, "postgres"),
		dest:          sqlx.NewDb(dest, "postgres"),
		listenErrChan: make(chan error),
		storage:       syncmap.New(),
	}
	if s.options.Chunk <= 0 {
		s.options.Chunk = 1000
	}

	if len(o) > 0 {
		s.options = o[0]

		if len(s.options.ExcludeTables) != 0 {
			getTablesQuery += " AND table_name NOT IN ('" + strings.Join(s.options.ExcludeTables, `','`) + "')"
		}
		if len(s.options.IncludeTables) != 0 {
			getTablesQuery += " AND table_name IN ('" + strings.Join(s.options.IncludeTables, `','`) + "')"
		}
	}

	return s
}

func (s *Sync) ListenError() <-chan error {
	return s.listenErrChan
}

const getPrimaryKeyQuery = `
   SELECT
      pg_attribute.attname
   FROM pg_index, pg_class, pg_attribute, pg_namespace
   WHERE
      pg_class.oid = '%s'::regclass
   AND
      indrelid = pg_class.oid
   AND
      nspname = 'public'
   AND
      pg_class.relnamespace = pg_namespace.oid
   AND
      pg_attribute.attrelid = pg_class.oid
   AND
      pg_attribute.attnum = any(pg_index.indkey)
   AND
      indisprimary
  `

func (s *Sync) tables() ([]string, error) {
	var tables []string
	err := s.src.Select(&tables, getTablesQuery)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *Sync) Run() {
	b, err := ioutil.ReadFile(".storage")
	if err != nil {
		s.listenErrChan <- err
	}

	err = s.storage.LoadFromByte(b)
	if err != nil {
		s.listenErrChan <- err
	}

	tables, err := s.tables()
	if err != nil {
		s.listenErrChan <- err
		return
	}

	for _, table := range tables {
		go s.sync(table)
	}
	s.persist()
}

func (s *Sync) persist() {
	for range time.Tick(time.Second * 5) {
		b, err := s.storage.ToByte()
		if err != nil {
			s.listenErrChan <- err
			continue
		}
		err = ioutil.WriteFile(".storage", b, 0644)
		if err != nil {
			s.listenErrChan <- err
			continue
		}
	}
}

func (s *Sync) sync(tableName string) {
	var primaryKey string

	err := s.src.Get(&primaryKey, fmt.Sprintf(getPrimaryKeyQuery, tableName))
	if err != nil {
		s.listenErrChan <- fmt.Errorf("[sync:%s] %s", tableName, err.Error())
		return
	}

	for range time.Tick(s.options.SyncInterval) {
		pgsyncLastTableSync.WithLabelValues(tableName).Set(float64(time.Now().Unix()))
		rows, err := s.src.Queryx(
			fmt.Sprintf("SELECT * FROM %s WHERE %s > %v LIMIT %d", tableName, primaryKey, s.storage.Load(tableName), s.options.Chunk),
		)
		if err != nil {
			s.listenErrChan <- fmt.Errorf("[sync:%s] failed to query table > %s", tableName, err.Error())
			continue
		}
		defer rows.Close()

		s.listenErrChan <- fmt.Errorf("[sync:%s] syncing with %s > %v", tableName, primaryKey, s.storage.Load(tableName))

		for rows.Next() {
			m := make(map[string]interface{})
			err = rows.MapScan(m)
			if err != nil {
				s.listenErrChan <- fmt.Errorf("[sync:%s] failed to perform MapScan > %s", tableName, err.Error())
				continue
			}

			var (
				counter = 1
				iter    []string
				keys    []string
				vals    []interface{}
			)
			for key, val := range m {
				iter = append(iter, fmt.Sprintf("$%d", counter))
				counter++

				keys = append(keys, key)
				vals = append(vals, val)
			}

			_, err = s.dest.Exec(
				fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(keys, ","), strings.Join(iter, ",")),
				vals...,
			)
			if err != nil {
				s.listenErrChan <- fmt.Errorf("[sync:%s] failed to insert values > %s", tableName, err.Error())
				continue
			}
			s.storage.Store(tableName, m[primaryKey])
		}
	}
}
