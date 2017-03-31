// oracle
package main

import (
	//"encoding/hex"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	//"github.com/mattn/davfs"
	"golang.org/x/net/webdav"
	//"github.com/golang/net/webdav"
	"gopkg.in/errgo.v1"
	//oradrv "gopkg.in/goracle.v1/oracle"
	"gopkg.in/rana/ora.v4"
)

type Driver struct {
}

type FileSystem struct {
	//	dbconn   string
	//	username string
	//	userpass string
	//	conn     *oradrv.Connection
	mu    sync.Mutex
	db    Db
	Debug bool
}

type FileInfo struct {
	name     string
	size     int64
	mode     os.FileMode
	mod_time time.Time
}

type File struct {
	fs       *FileSystem
	name     string
	off      int64
	size     int64
	body     []byte
	children []os.FileInfo
}

func (d *Driver) Mount(sid, username, userpass string, timeout time.Duration, debug bool) (webdav.FileSystem, error) {
	db := NewDb(username, userpass, sid, timeout)
	// Проверяем корректность имени пользователя, пароля и строки соединения
	if err := db.Do(func(ses *ora.Ses) error { return nil }); err != nil {
		return nil, err
	}
	return &FileSystem{
		db:    db,
		Debug: debug,
	}, nil
}

func (d *Driver) CreateFS(source string) error {
	//	db, err := sql.Open("sqlite3", source)
	//	if err != nil {
	//		return err
	//	}
	//	defer db.Close()
	//	_, err = db.Exec(createSQL)
	//	if err != nil {
	//		return err
	//	}
	return nil
}

func clearName(name string) (string, error) {
	slashed := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if !strings.HasSuffix(name, "/") && slashed {
		name += "/"
	}
	if !strings.HasPrefix(name, "/") {
		return "", os.ErrInvalid
	}
	return name, nil
}

func (fs *FileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	//	fs.mu.Lock()
	//	defer fs.mu.Unlock()

	//	if fs.Debug {
	//		log.Printf("FileSystem.Mkdir %v", name)
	//	}

	//	if !strings.HasSuffix(name, "/") {
	//		name += "/"
	//	}

	//	var err error
	//	if name, err = clearName(name); err != nil {
	//		return err
	//	}

	//	_, err = fs.stat(name)
	//	if err == nil {
	//		return os.ErrExist
	//	}

	//	base := "/"
	//	for _, elem := range strings.Split(strings.Trim(name, "/"), "/") {
	//		base += elem + "/"
	//		_, err = fs.stat(base)
	//		if err != os.ErrNotExist {
	//			return err
	//		}
	//		_, err = fs.db.Exec(`insert into filesystem(name, content, mode, mod_time) values($1, '', $2, current_timestamp)`, base, perm.Perm()|os.ModeDir)
	//		if err != nil {
	//			return err
	//		}
	//	}
	//	return nil
	return errgo.New("Функция создания папки не поддерживается")
}

func (fs *FileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.Debug {
		log.Printf("FileSystem.OpenFile %v", name)
	}

	var err error
	if name, err = clearName(name); err != nil {
		return nil, err
	}

	if flag&os.O_CREATE != 0 {
		// file should not have / suffix.
		if strings.HasSuffix(name, "/") {
			return nil, os.ErrInvalid
		}
		// based directory should be exists.
		dir, _ := path.Split(name)
		_, err := fs.stat(dir)
		if err != nil {
			return nil, os.ErrInvalid
		}
		_, err = fs.stat(name)
		if err == nil {
			if flag&os.O_EXCL != 0 {
				return nil, os.ErrExist
			}
			fs.removeAll(name)
		}

		if err := fs.db.Do(func(ses *ora.Ses) error {
			if _, err = ses.PrepAndExe(`begin webdav.create_file(:1); end;`, name); err != nil {
				fmt.Println("--------\nCreate", err.Error())
				return err
			}
			return nil

		}); err != nil {
			return nil, err
		}
		return &File{fs, name, 0, 0, nil, nil}, nil
	}

	fi, err := fs.stat(name)
	if err != nil {
		return nil, os.ErrNotExist
	}
	if !strings.HasSuffix(name, "/") && fi.IsDir() {
		name += "/"
	}
	return &File{fs, name, 0, fi.Size(), nil, nil}, nil
}

func (fs *FileSystem) removeAll(name string) error {
	var err error
	if name, err = clearName(name); err != nil {
		return err
	}

	if fi, err := fs.stat(name); err != nil || fi.IsDir() {
		return os.ErrPermission
	}

	if err := fs.db.Do(func(ses *ora.Ses) error {
		if _, err = ses.PrepAndExe(`begin webdav.delete_file(:1); end;`, name); err != nil {
			fmt.Println("--------\nDelete", err.Error())
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (fs *FileSystem) RemoveAll(ctx context.Context, name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.Debug {
		log.Printf("FileSystem.RemoveAll %v", name)
	}

	return fs.removeAll(name)
}

func (fs *FileSystem) Rename(ctx context.Context, oldName, newName string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.Debug {
		log.Printf("FileSystem.Rename %v %v", oldName, newName)
	}

	var err error
	if oldName, err = clearName(oldName); err != nil {
		return err
	}
	if newName, err = clearName(newName); err != nil {
		return err
	}

	of, err := fs.stat(oldName)
	if err != nil {
		return os.ErrExist
	}
	if of.IsDir() && !strings.HasSuffix(oldName, "/") {
		oldName += "/"
		newName += "/"
	}

	_, err = fs.stat(newName)
	if err == nil {
		return os.ErrExist
	}
	return fs.db.Do(func(ses *ora.Ses) error {
		if _, err = ses.PrepAndExe(`begin webdav.rename_file(:1, :2); end;`, oldName, newName); err != nil {
			fmt.Println("--------\nRename", err.Error())
			return err
		}
		return nil
	})
}

func (fs *FileSystem) stat(name string) (os.FileInfo, error) {
	var err error
	if name, err = clearName(name); err != nil {
		return nil, err
	}

	if name == "/" {
		return &FileInfo{

			name:     "/",
			size:     0,
			mode:     os.ModeDir,
			mod_time: time.Now(),
		}, nil
	}

	if strings.HasSuffix(name, "/") {
		name = name[:len(name)-1]
	}
	dir, fname := path.Split(path.Clean(name))
	if _, ok := skippedNames[fname]; ok {
		return nil, os.ErrNotExist
	}

	var fi FileInfo
	if err := fs.db.Do(func(ses *ora.Ses) error {
		// fetch records
		stm, err := ses.Prep(`select fname, fsize, to_char(fmode), fmodified from table(webdav.dir(:1)) where fname = :2`, ora.S, ora.U64, ora.S, ora.T)
		defer stm.Close()
		if err != nil {
			return err
		}
		rset, err := stm.Qry(dir, name)
		if err != nil {
			return err
		}

		for rset.Next() {
			fi = FileInfo{
				name:     rset.Row[0].(string),
				size:     int64(rset.Row[1].(uint64)),
				mod_time: rset.Row[3].(time.Time).UTC(),
			}
			if rset.Row[2].(string) != "0" {
				fi.mode = os.ModeDir
			}
			return nil
		}
		return os.ErrNotExist
	}); err != nil {
		return nil, err
	}
	_, fi.name = path.Split(path.Clean(fi.name))
	if fi.name == "" {
		fi.name = "/"
	}
	return &fi, nil
}

func (fs *FileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.Debug {
		log.Printf("FileSystem.Stat %v", name)
	}

	return fs.stat(name)
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.mod_time }
func (fi *FileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *FileInfo) Sys() interface{}   { return nil }

func (f *File) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	f.body = nil
	if f.fs.Debug {
		log.Printf("File.Write %v", f.name)
	}
	bufLen := uint64(len(p))
	if err := f.fs.db.Do(func(ses *ora.Ses) error {
		if _, err := ses.PrepAndExe(`begin webdav.write_file(:1, :2, :3, :4); end;`, f.name, 1+f.off, bufLen, p[:bufLen]); err != nil {
			fmt.Println("--------\nWrite", err.Error())
			return err
		}
		f.off += int64(bufLen)
		return nil
	}); err != nil {
		return 0, err
	}

	return int(bufLen), nil
}

func (f *File) Close() error {
	if f.fs.Debug {
		log.Printf("File.Close %v", f.name)
	}
	f.body = nil
	return nil
}

func (f *File) Read(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.fs.Debug {
		log.Printf("File.Read %v. offset = %v, len = %v", f.name, 1+f.off, len(p))
	}

	err := func() error {
		if f.body == nil {
			if err := f.fs.db.Do(func(ses *ora.Ses) error {
				stm, err := ses.Prep(`begin webdav.read_file_all(:1, :2); end;`, ora.S, ora.L)
				defer stm.Close()
				if err != nil {
					return err
				}
				lob := ora.Lob{}
				_, err = stm.Exe(&f.name, &lob)
				if err != nil {
					return err
				}
				f.body, err = lob.Bytes()
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}()

	if err != nil {
		return 0, err
	}
	r := bytes.NewReader(f.body)
	bl, err := r.ReadAt(p, f.off)
	if err != nil {
		return bl, err
	}
	f.off += int64(bl)
	return bl, nil
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.fs.Debug {
		log.Printf("File.Readdir %v", f.name)
	}

	if f.children == nil {
		if err := f.fs.db.Do(func(ses *ora.Ses) error {
			stm, err := ses.Prep(`select fname, fsize, to_char(fmode), fmodified from table(webdav.dir(:1))`, ora.S, ora.U64, ora.S, ora.T)
			defer stm.Close()
			if err != nil {
				return err
			}
			rset, err := stm.Qry(&f.name)
			if err != nil {
				return err
			}
			f.children = []os.FileInfo{}
			for rset.Next() {
				fi := &FileInfo{
					name:     rset.Row[0].(string),
					size:     int64(rset.Row[1].(uint64)),
					mod_time: rset.Row[3].(time.Time),
				}
				if rset.Row[2].(string) != "0" {
					fi.mode = os.ModeDir
				}

				_, fi.name = path.Split(path.Clean(fi.name))
				if fi.name == "" {
					fi.name = "/"
				}

				f.children = append(f.children, fi)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	old := f.off
	if old >= int64(len(f.children)) {
		if count > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}
	if count > 0 {
		f.off += int64(count)
		if f.off > int64(len(f.children)) {
			f.off = int64(len(f.children))
		}
	} else {
		f.off = int64(len(f.children))
		old = 0
	}
	return f.children[old:f.off], nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.fs.Debug {
		log.Printf("File.Seek %v %v %v", f.name, offset, whence)
	}

	var err error
	switch whence {
	case 0:
		f.off = 0
	case 2:
		//		Убрано для оптимизации скорости
		//		if fi, err := f.fs.stat(f.name); err != nil {
		//			return 0, err
		//		} else {
		//			f.off = fi.Size()
		//		}
		f.off = f.size
	}
	f.off += offset

	return f.off, err
}

func (f *File) Stat() (os.FileInfo, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.fs.Debug {
		log.Printf("File.Stat %v", f.name)
	}

	return f.fs.stat(f.name)
}

func (f *File) ReadFrom(r io.Reader) (n int64, err error) {
	if f.fs.Debug {
		log.Printf("File.ReadFrom %v", f.name)
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	if err := f.fs.db.Do(func(ses *ora.Ses) error {

		stm, err := ses.Prep(`begin webdav.write_file_all(:1, :2); end;`, ora.S, ora.L)
		defer stm.Close()
		if err != nil {
			return err
		}
		lob := ora.Lob{Reader: bytes.NewReader(b)}
		_, err = stm.Exe(&f.name, &lob)
		if err != nil {
			fmt.Println("--------\nWrite", err.Error())
			return err
		}
		f.body = b
		f.off = int64(len(b))
		f.size = f.off
		return nil
	}); err != nil {
		return 0, err
	}
	return int64(len(b)), nil
}

var skippedNames map[string]bool = map[string]bool{
	"Thumbs.db":   true,
	"folder.gif":  true,
	"folder.jpg":  true,
	"desktop.ini": true,
}
