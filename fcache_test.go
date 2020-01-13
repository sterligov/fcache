package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	port    = "3333"
	maxConn = 10
)

func TestMain(m *testing.M) {
	go func() {
		server := &Fcache{
			Port:           port,
			MaxConnections: maxConn,
			Journal:        nil,
		}
		Run(server)
	}()
	time.Sleep(time.Second) // wait server
	m.Run()
}

func TestCreateAndRead(t *testing.T) {
	c := &FcacheClient{}
	if err := c.Connect("127.0.0.1:"+port, time.Millisecond*10); err != nil {
		t.Error(err)
		return
	}

	if err := c.Set("test_key", "test_val", 1); err != nil {
		t.Error(err)
		return
	}

	v, err := c.Get("test_key")
	if err != nil {
		t.Error(err)
		return
	}

	if v != "test_val" {
		t.Errorf(`Expected "test_val", got %s`, v)
		return
	}

	c.Close()
}

func TestDelete(t *testing.T) {
	c := &FcacheClient{}
	if err := c.Connect("127.0.0.1:"+port, time.Millisecond*10); err != nil {
		t.Error(err)
		return
	}

	if err := c.Set("test_key_2", "test_val_2", 1); err != nil {
		t.Error(err)
		return
	}

	if err := c.Delete("test_key_2"); err != nil {
		t.Error(err)
		return
	}

	v, err := c.Get("test_key_2")
	if err == nil || v != "" || err.Error() != "Fcache: key not found" {
		t.Error(`Expected error message "Fcache: key not found"`)
		return
	}

	err = c.Delete("test_key_2")
	if err == nil || err.Error() != "Fcache: key not found" {
		t.Error(`Expected error message "Fcache: key not found"`)
		return
	}

	c.Close()
}

func TestExpireTime(t *testing.T) {
	c := &FcacheClient{}
	if err := c.Connect("127.0.0.1:"+port, time.Second*2); err != nil {
		t.Error(err)
		return
	}

	if err := c.Set("test_key_3", "test_val_3", 1); err != nil {
		t.Error(err)
		return
	}

	if err := c.Set("test_key_4", "test_val_4", 0); err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second)

	v, err := c.Get("test_key_2")
	if err == nil || v != "" || err.Error() != "Fcache: key not found" {
		t.Errorf(`Expected error message "Fcache: key not found", got %s`, err.Error())
		return
	}

	v, err = c.Get("test_key_4")
	if err != nil {
		t.Error(err)
		return
	}

	c.Close()
}

func TestConnectionTimeout(t *testing.T) {
	c := &FcacheClient{}
	if err := c.Connect("127.0.0.1:"+port, time.Millisecond*5); err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Millisecond * 10)
	if err := c.Set("test_key", "test_val", 1); err == nil {
		t.Error("Expected timeout error")
	}
	c.Close()
}

func TestMaxConnections(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(maxConn)
	for i := 0; i < maxConn; i++ {
		go func() {
			defer wg.Done()
			c := &FcacheClient{}
			if err := c.Connect("127.0.0.1:"+port, time.Millisecond*100); err != nil {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()

	c := &FcacheClient{}
	if err := c.Connect("127.0.0.1:"+port, time.Millisecond*50); err == nil || err.Error() != "Connection limit exceeded" {
		t.Errorf(`Expected error message "Connection limit exceeded", got %s`, err.Error())
		return
	}
	time.Sleep(time.Second)
}

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
