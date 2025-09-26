package handler

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type pushMetric struct {
	config *util.Config
	db     *util.DB
}

func GetPushMetricHandler(config *util.Config, db *util.DB) func(w http.ResponseWriter, r *http.Request) {
	return wrap(pushMetric{config, db})
}

type pushMetricInput struct {
	MetricName  string `json:"name"`
	MetricValue string `json:"value"`
	Cluster     string `json:"cluster"`
	Context     string `json:"context"`
	Node        string `json:"node"`
	Xname       string `json:"xname"`
	JobId       string `json:"jobid"`
	Timestamp   int64  `json:"timestamp"`
}

func (h pushMetric) Post(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)

	// allow calling this endpoint only within CSCS network
	ip_addr := r.Header.Get("X-Forwarded-For")
	if ip_addr == "" {
		// direct connection to the deployment
		ip_addr = strings.Split(r.RemoteAddr, ":")[0]
	}
	parsed_ip := net.ParseIP(ip_addr)
	if parsed_ip == nil {
		msg := "You are not allowed to push metric data"
		logger.Debug().Msgf("parsed_ip is nil, therefore pushing metric data is blocked. X-Original-Forwarded-For=%v X-Forwarded-For=%v req.RemoteAddr=%v",
			r.Header.Get("X-Original-Forwarded-For"),
			r.Header.Get("X-Forwarded-For"),
			r.RemoteAddr)
		pie(logger.Error, condition_error{msg}, "", http.StatusBadRequest)
	}
	// check if it is in a valid subnet
	_, subnet1, _ := net.ParseCIDR("148.187.0.0/16")
	_, subnet2, _ := net.ParseCIDR("172.16.0.0/12")
	if false == subnet1.Contains(parsed_ip) && false == subnet2.Contains(parsed_ip) {
		msg := "You are not allowed to use push metric data"
		logger.Debug().Msgf("parsed_ip is %v, but is not within the allowed subnets, therefore pushing metric data is blocked", parsed_ip)
		pie(logger.Error, condition_error{msg}, "", http.StatusBadRequest)
	}

	var inData pushMetricInput
	err := json.NewDecoder(r.Body).Decode(&inData)
	pie(logger.Warn, err, "", http.StatusBadRequest)

	if inData.MetricName == "" {
		pie(logger.Warn, condition_error{"Field `name` is missing"}, "", http.StatusBadRequest)
	}
	if inData.MetricValue == "" {
		pie(logger.Warn, condition_error{"Field `value` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Cluster == "" {
		pie(logger.Warn, condition_error{"Field `cluster` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Context == "" {
		pie(logger.Warn, condition_error{"Field `context` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Node == "" {
		pie(logger.Warn, condition_error{"Field `node` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Xname == "" {
		pie(logger.Warn, condition_error{"Field `xname` is missing"}, "", http.StatusBadRequest)
	}
	if inData.JobId == "" {
		pie(logger.Warn, condition_error{"Field `jobid` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Timestamp == 0 {
		inData.Timestamp = time.Now().Unix()
	}

	if !h.db.PushMetricData(inData.Timestamp, inData.JobId, inData.MetricName, inData.MetricValue, inData.Xname, inData.Node, inData.Context, inData.Cluster) {
		logger.Error().Msgf("Failed pushing custom userdata to database")
		w.Write([]byte("Failed pushing custom userdata to database"))
	} else {
		w.Write([]byte("ok"))
	}
}
