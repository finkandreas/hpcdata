package handler

import (
	"net/http"
	"time"
)

type test struct{}

func GetTestHandler() func(w http.ResponseWriter, r *http.Request) {
	return wrap(test{})
}

func (h test) Get(w http.ResponseWriter, r *http.Request) {
	time.Sleep(300 * time.Second)
	_, _ = w.Write([]byte("ok"))
}
