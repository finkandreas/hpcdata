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

	sleep_time := r.URL.Query().Get("sleep")
	logger.Debug().Msgf("Received sleep_time=%v from request", resp_code)
	sleep_time_int := 1
	if sleep_time != "" {
		var err error
		if sleep_time_int, err = strconv.Atoi(sleep_time); err != nil {
			logger.Warn().Err(err).Msgf("Could not convert to an integer")
			sleep_time_int = 1
		}
	}

	time.Sleep(time.Duration(sleep_time_int) * time.Second)
	http.Error(w, resp_code, resp_code_int)
}
