package main

import (
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
