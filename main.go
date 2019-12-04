package main

import (
	"flag"
)

func main() {
	port := flag.String("port", "3333", "TCP port")
	// memSize := flag.Int64("maxmem", 1024*1024, "Maximum memory size for cache")
	maxConnections := flag.Int64("maxconn", 10, "Maximum number of concurrent connections")
	timeout := flag.Int64("timeout", 30, "Connection timeout in seconds")
	journal := flag.String("journal", "", "Journal file")
	restore := flag.Bool("restore", false, "Restore data from journal")

	flag.Parse()

	server := &Fcache{
		MaxConnections:    *maxConnections,
		Port:              *port,
		ConnectionTimeout: *timeout,
		Journal: &Journal{
			Filename: *journal,
			Restore:  *restore,
		},
	}
	Run(server)
}
