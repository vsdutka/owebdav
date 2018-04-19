// owebdav
package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/mattn/go-colorable"

	//"github.com/mattn/davfs"
	"github.com/vsdutka/wlog"
	"golang.org/x/net/webdav"
)

var (
	addr         = flag.String("addr", ":9999", "server address")
	sid          = flag.String("sid", "", "Oracle SID")
	sess_timeout = flag.Duration("session_timeout", 30*time.Second, "session timeout")
	certFileName = flag.String("cert", "cert.pem", "certificate file name")
	keyFileName  = flag.String("key", "key.pem", "key file name")
	debug        = flag.Bool("debug", false, "debug")
)

func errorString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func main() {
	flag.Parse()

	log.SetOutput(colorable.NewColorableStderr())
	handler := http.HandlerFunc(wlog.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="davfs"`)
			http.Error(w, "authorization failed", http.StatusUnauthorized)
			return
		}

		dav, err := getDav(username, password)

		if err != nil {
			log.Print(err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		dav.ServeHTTP(w, r)
	}))

	log.Print(color.CyanString("Server started %v", *addr))
	http.Handle("/", handler)
	//	cfg := &tls.Config{
	//		MinVersion:               tls.VersionTLS12,
	//		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
	//		PreferServerCipherSuites: true,
	//		CipherSuites: []uint16{
	//			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	//			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	//			//tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	//			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	//		},
	//	}
	//	srv := &http.Server{
	//		Addr:         *addr,
	//		TLSConfig:    cfg,
	//		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
	//	}
	//	log.Fatal(srv.ListenAndServeTLS(*certFileName, *keyFileName))

	log.Fatal(http.ListenAndServeTLS(*addr, *certFileName, *keyFileName, nil))
}

var (
	mu   sync.Mutex
	davs map[string]*webdav.Handler = make(map[string]*webdav.Handler)
)

func getDav(username, password string) (*webdav.Handler, error) {
	mu.Lock()
	defer mu.Unlock()

	if dav, ok := davs[strings.ToUpper(username+" "+password)]; ok {
		return dav, nil
	}

	d := &Driver{}
	fs, err := d.Mount(*sid, username, password, *sess_timeout, *debug)
	if err != nil {
		return nil, err
	}

	dav := &webdav.Handler{
		FileSystem: fs,
		LockSystem: webdav.NewMemLS(),

		Logger: func(r *http.Request, err error) {
			litmus := r.Header.Get("X-Litmus")
			if len(litmus) > 19 {
				litmus = litmus[:16] + "..."
			}

			switch r.Method {
			case "COPY", "MOVE":
				dst := ""
				if u, err := url.Parse(r.Header.Get("Destination")); err == nil {
					dst = u.Path
				}
				log.Printf("%-18s %s %s %s",
					color.GreenString(r.Method),
					r.URL.Path,
					dst,
					color.RedString(errorString(err)))
			default:
				log.Printf("%-18s %s %s",
					color.GreenString(r.Method),
					r.URL.Path,
					color.RedString(errorString(err)))
			}
		},
	}
	davs[strings.ToUpper(username+" "+password)] = dav
	return dav, nil

}
