package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
)

type FcacheClient struct {
	conn net.Conn
}

func (fc *FcacheClient) Connect(addr string, timeout time.Duration) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	fc.conn = conn
	time.AfterFunc(timeout, func() {
		fc.conn.Close()
	})

	if err := fc.isOK(); err != nil {
		fc.Close()
		return err
	}

	return nil
}

func (fc *FcacheClient) Close() error {
	return fc.conn.Close()
}

func (fc *FcacheClient) Set(key, value string, expire int64) error {
	e := strconv.FormatInt(expire, 10)
	_, err := fmt.Fprintf(fc.conn, "set\n%s\n%s\n%s\n", key, value, e)
	if err != nil {
		return err
	}

	return fc.isOK()
}

func (fc *FcacheClient) Get(key string) (string, error) {
	_, err := fmt.Fprintf(fc.conn, "get\n%s\n", key)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(fc.conn)
	val, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	val = val[:len(val)-1]
	if val != "ok" {
		return "", fmt.Errorf(val)
	}

	val, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return val[:len(val)-1], nil
}

func (fc *FcacheClient) Delete(key string) error {
	_, err := fmt.Fprintf(fc.conn, "delete\n%s\n", key)
	if err != nil {
		return err
	}

	return fc.isOK()
}

func (fc *FcacheClient) isOK() error {
	reader := bufio.NewReader(fc.conn)
	val, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	val = val[:len(val)-1]
	if val != "ok" {
		return fmt.Errorf(val)
	}

	return nil
}
