package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alileza/pgsync/sync"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	verbose                = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
	databaseSourceDSN      = kingpin.Flag("src", "database source datasource name").Short('i').Required().String()
	databaseDestinationDSN = kingpin.Flag("dest", "database destination datasource name").Short('d').Required().String()
	exclude                = kingpin.Flag("exclude", "exclude some tables").Short('x').String()
	only                   = kingpin.Flag("only", "select specific tables").Short('s').String()
	interval               = kingpin.Flag("sync_interval", "").Default("1m").Short('t').Duration()
	prom                   = kingpin.Flag("prometheus_port", "").Short('p').String()
	chunk                  = kingpin.Flag("chunk", "fetch query amount").Short('c').Int()
)

func main() {
	kingpin.Parse()

	os.Exit(Main(
		log.New(os.Stdout, "", 0),
		log.New(os.Stderr, "", 0),
	))
}

func Main(o, e *log.Logger) int {
	src, err := sql.Open("postgres", *databaseSourceDSN)
	if err != nil {
		e.Println(err)
		return 1
	}
	defer src.Close()

	dest, err := sql.Open("postgres", *databaseDestinationDSN)
	if err != nil {
		e.Println(err)
		return 1
	}
	defer src.Close()

	s := sync.NewSync(src, dest, &sync.Options{
		SyncInterval:  *interval,
		Chunk:         *chunk,
		IncludeTables: strings.Split(*only, ","),
		ExcludeTables: strings.Split(*exclude, ","),
	})
	go s.Run()
	go func() {
		for {
			if err := <-s.ListenError(); err != nil {
				e.Println(err.Error())
			}
		}
	}()

	if *prom != "" {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/metrics", http.StatusFound)
		})
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(*prom, nil); err != nil {
			log.Println(err)
		}
	}

	term := make(chan os.Signal)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	select {
	case <-term:
		o.Println("Received SIGTERM, exiting gracefully...")
	}

	return 0
}
