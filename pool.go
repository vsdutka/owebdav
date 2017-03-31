// pool
package main

import (
	"fmt"
	"sync"
	"time"

	//"gopkg.in/goracle.v1/oracle"
	"gopkg.in/rana/ora.v4"
)

type Db interface {
	Do(fn func(ses *ora.Ses) error) error
}

type db struct {
	username string
	userpass string
	sid      string
	timeout  time.Duration
	env      *ora.Env
	srv      *ora.Srv
	ses      *ora.Ses
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

func (p *db) Do(fn func(ses *ora.Ses) error) error {
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
							p.closeSes()
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

	p.closeSes()

	if p.ses == nil {
		p.env, p.srv, p.ses, err = ora.NewEnvSrvSes(fmt.Sprintf("%s/%s@%s", p.username, p.userpass, p.sid))
		if err != nil {
			p.closeSes()
			return err
		}
	}
	return fn(p.ses)
}

func (p *db) closeSes() {
	if p.ses != nil {
		p.ses.Close()
		p.ses = nil
	}
	if p.srv != nil {
		p.srv.Close()
		p.srv = nil
	}
	if p.env != nil {
		p.env.Close()
		p.env = nil
	}
}

//env, err := ora.OpenEnv()
//	defer env.Close()
//	if err != nil {
//		panic(err)
//	}
//	srvCfg := ora.SrvCfg{Dblink: "orcl"}
//	srv, err := env.OpenSrv(&srvCfg)
//	defer srv.Close()
//	if err != nil {
//		panic(err)
//	}
//	sesCfg := ora.SesCfg{
//		Username: "test",
//		Password: "test",
//	}
//	ses, err := srv.OpenSes(sesCfg)
//	defer ses.Close()
//	if err != nil {
//		panic(err)
//	}
