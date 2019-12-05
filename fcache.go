package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	journalFolderName = "journal"
	methodSet         = "set"
	methodGet         = "get"
	methodDelete      = "delete"
)

var cacheMethod = map[string]func(*Fcache, *request){
	methodGet:    (*Fcache).get,
	methodSet:    (*Fcache).set,
	methodDelete: (*Fcache).delete,
}

type Journal struct {
	Filename string
	Restore  bool
	channel  chan *request
}

type Fcache struct {
	MaxMemSize        int64
	MaxConnections    int64
	Port              string
	ConnectionTimeout int64 // second
	// memSize     int64
	hub         map[string]cacheValue
	Journal     *Journal
	requestChan chan *request
}

type cacheValue struct {
	value       string
	deleteTimer *time.Timer
}

type request struct {
	Method string
	Key    string
	Value  string
	Expire int64 // seconds
	conn   net.Conn
}

func Run(f *Fcache) {
	f.hub = make(map[string]cacheValue)
	f.requestChan = make(chan *request)
	if f.ConnectionTimeout == 0 {
		f.ConnectionTimeout = 30
	}
	if f.Journal != nil {
		f.Journal.channel = make(chan *request)
		if f.Journal.Restore {
			if f.Journal.Filename == "" {
				f.Journal.Filename = findLastJournalFile()
			}
			f.restoreFromJournal()
		}
		go f.processJournal()
	}

	go f.processRequest()
	f.run()
}

func (f *Fcache) run() {
	listener, err := net.Listen("tcp", ":"+f.Port)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Println(err)
		}
	}()

	guard := make(chan struct{}, f.MaxConnections)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		select {
		case guard <- struct{}{}:
			fmt.Fprintf(conn, "ok\n")
			t := time.Duration(f.ConnectionTimeout) * time.Second
			time.AfterFunc(t, func() {
				conn.Close()
			})
		case <-time.After(time.Millisecond):
			fmt.Fprintf(conn, "Connection limit exceeded\n")
			if err := conn.Close(); err != nil {
				log.Println(err)
			}
			continue
		}

		go func() {
			f.handler(conn)
			<-guard
		}()
	}
}

func (f *Fcache) handler(conn net.Conn) {
	for {
		req, err := parseRequest(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(conn, "%s\n", err.Error())
			}
			if err := conn.Close(); err != nil {
				log.Println(err)
			}
			return
		}
		if f.Journal != nil && req.Method != methodGet {
			go func() {
				f.Journal.channel <- req
			}()
		}
		f.requestChan <- req
	}
}

func parseRequest(conn net.Conn) (*request, error) {
	var err error
	req := &request{conn: conn}
	reader := bufio.NewReader(conn)

	if req.Method, err = reader.ReadString('\n'); err != nil {
		return nil, err
	}

	req.Method = req.Method[:len(req.Method)-1]
	if req.Method != methodGet && req.Method != methodSet && req.Method != methodDelete {
		return nil, fmt.Errorf("Unknown command")
	}

	if req.Key, err = reader.ReadString('\n'); err != nil {
		return nil, err
	}
	req.Key = req.Key[:len(req.Key)-1]

	if req.Method == methodSet {
		if req.Value, err = reader.ReadString('\n'); err != nil {
			return nil, err
		}
		req.Value = req.Value[:len(req.Value)-1]

		var expire string
		if expire, err = reader.ReadString('\n'); err != nil {
			return nil, err
		}
		expire = expire[:len(expire)-1]
		if req.Expire, err = strconv.ParseInt(expire, 10, 64); err != nil {
			return nil, err
		}
	}

	return req, nil
}

func (f *Fcache) processRequest() {
	for req := range f.requestChan {
		cacheMethod[req.Method](f, req)
	}
}

func (f *Fcache) delete(req *request) {
	var msg string
	if _, ok := f.hub[req.Key]; ok {
		if f.hub[req.Key].deleteTimer != nil {
			f.hub[req.Key].deleteTimer.Stop()
		}
		delete(f.hub, req.Key)
		msg = "ok\n"
	} else {
		msg = "Fcache: key not found\n"
	}
	if req.conn != nil {
		fmt.Fprintf(req.conn, msg)
	}
}

func (f *Fcache) get(req *request) {
	if v, ok := f.hub[req.Key]; ok {
		fmt.Fprintf(req.conn, "ok\n%s\n", v.value)
	} else {
		fmt.Fprintf(req.conn, "Fcache: key not found\n")
	}
}

func (f *Fcache) set(req *request) {
	cv := cacheValue{value: req.Value}
	if req.Expire != 0 {
		cv.deleteTimer = time.AfterFunc(time.Duration(req.Expire)*time.Second, func() {
			f.requestChan <- &request{
				Method: "delete",
				Key:    req.Key,
			}
		})
	}
	if _, ok := f.hub[req.Key]; ok && f.hub[req.Key].deleteTimer != nil {
		f.hub[req.Key].deleteTimer.Stop()
	}
	f.hub[req.Key] = cv
	fmt.Fprintf(req.conn, "ok\n")
}

func (f *Fcache) processJournal() {
	if f.Journal != nil {
		if f.Journal.Filename == "" {
			t := strconv.FormatInt(time.Now().Unix(), 10)
			f.Journal.Filename = filepath.Join(journalFolderName, "journal_"+t)
		}
		if _, err := os.Stat(journalFolderName); os.IsNotExist(err) {
			err := os.Mkdir(journalFolderName, 0755)
			if err != nil {
				panic(err)
			}
		}

		j, err := os.OpenFile(f.Journal.Filename, os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			panic(err)
		}
		defer j.Close()

		for req := range f.Journal.channel {
			r := *req
			if r.Expire != 0 {
				r.Expire = int64(time.Duration(r.Expire)*time.Second) + time.Now().UnixNano()
			}
			v, err := json.Marshal(r)
			if err != nil {
				log.Println(err)
				continue
			}
			if _, err := fmt.Fprintln(j, string(v)); err != nil {
				log.Println(err)
			}
		}
	}
}

func (f *Fcache) restoreFromJournal() {
	journalFile, err := os.Open(f.Journal.Filename)
	if err != nil {
		panic(err)
	}
	defer journalFile.Close()

	reader := bufio.NewReader(journalFile)
	for {
		record, err := reader.ReadString('\n')
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}

		record = record[:len(record)-1]
		req := &request{}
		err = json.Unmarshal([]byte(record), req)
		if err != nil {
			panic(err)
		}

		if req.Method == methodDelete {
			if _, ok := f.hub[req.Key]; ok && f.hub[req.Key].deleteTimer != nil {
				f.hub[req.Key].deleteTimer.Stop()
			}
			delete(f.hub, req.Key)
		} else if req.Method == methodSet {
			if time.Now().UnixNano() < req.Expire {
				t := time.Duration(time.Now().UnixNano() - req.Expire)
				f.hub[req.Key] = cacheValue{
					value: req.Value,
					deleteTimer: time.AfterFunc(t, func() {
						f.requestChan <- &request{
							Method: "delete",
							Key:    req.Key,
						}
					}),
				}
			} else if req.Expire == 0 {
				f.hub[req.Key] = cacheValue{
					value: req.Value,
				}
			}
		}
	}
}

func findLastJournalFile() string {
	dir, err := os.Open(journalFolderName)
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	journals, err := dir.Readdir(0)
	if err != nil {
		panic(err)
	}

	if len(journals) == 0 {
		panic("Journal folder is empty")
	}

	lastFile := journals[0]
	for i := 1; i < len(journals); i++ {
		if lastFile.ModTime().Unix() < journals[i].ModTime().Unix() {
			lastFile = journals[i]
		}
	}

	return filepath.Join(journalFolderName, lastFile.Name())
}
