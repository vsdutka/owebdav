// logger
package wlog

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/kardianos/osext"
)

type statusWriter struct {
	http.ResponseWriter
	status int
	length int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	w.length = len(b)
	return w.ResponseWriter.Write(b)
}

var logChan = make(chan string, 10000)

func init() {
	basePath := ".\\"
	exeName, err := osext.Executable()

	if err == nil {
		exeName, err = filepath.Abs(exeName)
		if err == nil {
			basePath = filepath.Dir(exeName)
		}
	}

	go func() {
		var (
			lastLogging = time.Time{}
			logFile     *os.File
			err         error
			str         string
		)
		defer func() {
			if logFile != nil {
				logFile.Close()
			}
		}()
		for {
			select {
			case str = <-logChan:
				{
					if lastLogging.Format("2006_01_02") != time.Now().Format("2006_01_02") {
						if logFile != nil {
							logFile.Close()
						}
						fileName := basePath + "\\log\\ex" + time.Now().Format("2006_01_02") + ".log"
						dir, _ := filepath.Split(fileName)
						os.MkdirAll(dir, os.ModeDir)

						logFile, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
						if err != nil {
							log.Fatalln(err)
						}
					}
					lastLogging = time.Now()
					logFile.WriteString(str)
				}
			}
		}
	}()
}
func writeToLog(msg string) {
	logChan <- msg
}

func HandlerFunc(h http.HandlerFunc) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		writer := statusWriter{w, 0, 0}
		h.ServeHTTP(&writer, r)
		end := time.Now()
		latency := end.Sub(start)
		statusCode := writer.status
		length := writer.length
		user, _, ok := r.BasicAuth()
		if !ok {
			user = "-"
		}
		url := r.URL.Path

		params := r.Form.Encode()
		if params != "" {
			url = url + "?" + params
		}

		writeToLog(fmt.Sprintf("%s, %s, %s, %s, %s, %s, %d, %d, %d, %d, %s, %s, %v\r\n",
			r.RemoteAddr,
			user,
			end.Format("2006.01.02"),
			end.Format("15:04:05.000000000"),
			r.Proto,
			r.Host,
			length,
			r.ContentLength,
			time.Since(start)/time.Millisecond,
			statusCode,
			r.Method,
			url,
			latency,
		))
	}
}
