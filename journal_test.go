package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJournalSave(t *testing.T) {
	port := "3334"
	go func() {
		server := &Fcache{
			Port:           port,
			MaxConnections: maxConn,
			Journal:        &Journal{},
		}
		Run(server)
	}()
	time.Sleep(time.Second) // wait server

	c := &FcacheClient{}
	c.Connect("127.0.0.1:"+port, time.Millisecond*50)
	c.Set("key_1", "val_1", 0)
	c.Get("key_1")
	c.Delete("key_1")
	c.Close()

	jf, err := os.Open(findLastJournalFile())
	if err != nil {
		t.Error(err)
		return
	}
	defer jf.Close()

	reader := bufio.NewReader(jf)
	s, err := reader.ReadString('\n')
	if err != nil {
		t.Error(err)
		return
	}
	s = s[:len(s)-1]
	expected := `{"Method":"set","Key":"key_1","Value":"val_1","Expire":0}`
	if s != expected {
		t.Errorf(`Error expected %s, got %s`, expected, s)
	}

	s, err = reader.ReadString('\n')
	if err != nil {
		t.Error(err)
		return
	}
	s = s[:len(s)-1]

	expected = `{"Method":"delete","Key":"key_1","Value":"","Expire":0}`
	if s != expected {
		t.Errorf(`Error expected %s, got %s`, expected, s)
	}
}

func TestJournalRestore(t *testing.T) {
	testData := []request{
		request{
			Method: methodSet,
			Key:    "key_1",
			Value:  "val_1",
			Expire: 0,
		},
		request{
			Method: methodDelete,
			Key:    "key_1",
			Value:  "val_1",
			Expire: 0,
		},
		request{
			Method: methodSet,
			Key:    "key_2",
			Value:  "val_2",
			Expire: 0,
		},
		request{
			Method: methodSet,
			Key:    "key_3",
			Value:  "val_3",
			Expire: time.Now().UnixNano() - 1,
		},
		request{
			Method: methodSet,
			Key:    "key_4",
			Value:  "val_4",
			Expire: time.Now().UnixNano() + int64(time.Second),
		},
	}
	if _, err := os.Stat(journalFolderName); os.IsNotExist(err) {
		os.Mkdir(journalFolderName, 0755)
	}
	testJournal := filepath.Join(journalFolderName, "test")
	journal, err := os.OpenFile(testJournal, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0775)
	if err != nil {
		t.Error(err)
		return
	}
	defer journal.Close()

	for _, t := range testData {
		v, _ := json.Marshal(t)
		fmt.Fprintln(journal, string(v))
	}

	port := "3335"
	go func() {
		server := &Fcache{
			Port:           port,
			MaxConnections: maxConn,
			Journal: &Journal{
				Filename: testJournal,
				Restore:  true,
			},
		}
		Run(server)
	}()
	time.Sleep(2 * time.Second) // wait server

	c := &FcacheClient{}
	c.Connect("127.0.0.1:"+port, time.Millisecond*50)
	if _, err := c.Get("key_1"); err == nil {
		t.Error("Expected not found error for key_1")
		return
	}
	if _, err := c.Get("key_2"); err != nil {
		t.Errorf("Expected val_2, got error %s", err.Error())
		return
	}
	if _, err := c.Get("key_3"); err == nil {
		t.Error("Expected not found error for key_3")
		return
	}
	if _, err := c.Get("key_4"); err == nil {
		t.Error("Expected not found error for key_4")
		return
	}
}
