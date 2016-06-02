// pool
package main

import (
	"sync"
	"time"

	"gopkg.in/goracle.v1/oracle"
)

type Db interface {
	Do(fn func(conn *oracle.Connection) error) error
}

type db struct {
	username string
	userpass string
	sid      string
	timeout  time.Duration
	conn     *oracle.Connection
	tm       *time.Timer
	sync.Mutex
}

func NewDb(username, userpass, sid string, timeout time.Duration) Db {
	return &db{
		username: username,
		userpass: userpass,
		sid:      sid,
		timeout:  timeout,
	}
}

func (p *db) Do(fn func(conn *oracle.Connection) error) error {
	var err error
	p.Lock()
	defer p.Unlock()
	if p.tm == nil {
		p.tm = time.NewTimer(p.timeout)
		go func() {
			for {
				select {
				case <-p.tm.C:
					{
						// Закрываем соединение с БД по таймауту неактивности
						func() {
							p.Lock()
							defer p.Unlock()
							if p.conn != nil {
								p.conn.Free(true)
								p.conn = nil
							}
							// Инициируем следующий тик через timeout
							p.tm.Reset(p.timeout)
						}()
					}
				}
			}

		}()
	}

	p.tm.Stop()
	defer p.tm.Reset(p.timeout)

	if p.conn != nil {
		if !p.conn.IsConnected() {
			p.conn.Free(true)
			p.conn = nil
		} else {
			if err := p.conn.Ping(); err != nil {
				p.conn.Close()
				p.conn.Free(true)
				p.conn = nil
			}
		}
	}

	if p.conn == nil {
		p.conn, err = oracle.NewConnection(p.username, p.userpass, p.sid, false)
		if err != nil {
			if p.conn != nil {
				p.conn.Free(true)
			}
			p.conn = nil
			return err
		}
	}
	return fn(p.conn)
}
