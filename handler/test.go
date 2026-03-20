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
	time.Sleep(1 * time.Second)
	http.Error(w, "Gateway timeout", http.StatusGatewayTimeout)
}
