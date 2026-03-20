package handler

import (
	"net/http"
	"strconv"
	"time"

	"cscs.ch/hpcdata/logging"
)

type test struct{}

func GetTestHandler() func(w http.ResponseWriter, r *http.Request) {
	return wrap(test{})
}

func (h test) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)

	resp_code := r.URL.Query().Get("response_code")
	logger.Debug().Msgf("Received resp_code=%v from request", resp_code)
	resp_code_int := http.StatusBadGateway
	if resp_code != "" {
		var err error
		if resp_code_int, err = strconv.Atoi(resp_code); err != nil {
			logger.Warn().Err(err).Msgf("Could not convert to an integer")
			resp_code_int = http.StatusBadGateway
		}
	}

	time.Sleep(1 * time.Second)
	http.Error(w, resp_code, resp_code_int)
}
