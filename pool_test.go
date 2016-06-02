// pool_test
package main

import (
	"flag"
	"testing"
	"time"

	"gopkg.in/goracle.v1/oracle"
)

var (
	test_dsn       = flag.String("dsn", "", "Oracle DSN (user/passw@sid)")
	test_dsn_user  string
	test_dsn_passw string
	test_dsn_sid   string
)

func init() {
	flag.Parse()
	test_dsn_user, test_dsn_passw, test_dsn_sid = oracle.SplitDSN(*test_dsn)
}

func TestReading(t *testing.T) {
	if !(*test_dsn != "") {
		t.Fatalf("cannot test connection without dsn!")
	}

	db := NewDb(test_dsn_user, test_dsn_passw, test_dsn_sid, 30*time.Second)

	err := db.Do(func(conn *oracle.Connection) error {
		cur := conn.NewCursor()
		defer cur.Close()

		if err := cur.Execute(`select fname, fsize, to_char(fmode), fmodified from table(webdav.dir(:1))`, []interface{}{"/Обращения/ФАС_453408/1/Фото повреждений/"}, nil); err != nil {
			return err
		}
		rows, err := cur.FetchAll()

		if err != nil {
			return err
		}

		for _, row := range rows {
			t.Log(row[0].(string))
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	<-time.After(40 * time.Second)
	err = db.Do(func(conn *oracle.Connection) error {
		cur := conn.NewCursor()
		defer cur.Close()

		if err := cur.Execute(`select fname, fsize, to_char(fmode), fmodified from table(webdav.dir(:1))`, []interface{}{"/Обращения/ФАС_453408/1/Фото повреждений/"}, nil); err != nil {
			return err
		}
		rows, err := cur.FetchAll()

		if err != nil {
			return err
		}

		for _, row := range rows {
			t.Log(row[0].(string))
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	<-time.After(40 * time.Second)
}
