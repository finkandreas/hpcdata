package handler

import (
	"net/http"
	"strconv"
	"time"
)

type test struct{}

func GetTestHandler() func(w http.ResponseWriter, r *http.Request) {
	return wrap(test{})
}

func (h test) Get(w http.ResponseWriter, r *http.Request) {
	resp_code := r.URL.Query().Get("response_code")
	resp_code_int := http.StatusBadGateway
	if resp_code != "" {
		var err error
		if resp_code_int, err = strconv.Atoi(resp_code); err != nil {
			resp_code_int = http.StatusBadGateway
		}
	}

	time.Sleep(1 * time.Second)
	http.Error(w, "Gateway timeout", resp_code_int)
}
