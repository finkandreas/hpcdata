package handler

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type customMetric struct {
	config *util.Config
	esclient *elastic.Client
	db     *util.DB
}

func GetCustomMetricHandler(config *util.Config, esclient *elastic.Client, db *util.DB) func(w http.ResponseWriter, r *http.Request) {
	return wrap(customMetric{config, esclient, db})
}

type customMetricOutput struct {
	MetricName  string `json:"name"`
	Context     string `json:"context"`
	Time        []int64 `json:"time"`
	MetricValue []string `json:"value"`
}
type customMetricOutputReturn map[string][]customMetricOutput

func (h customMetric) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)


	logger.Debug().Msgf("Passed all security checks to fetch GPU temperature data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	cluster := vars["system_name"]
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}

	metricName := r.URL.Query().Get("name")
	if metricName == "" {
		pie(logger.Warn, condition_error{"Query parameter `name` is mandatory"}, "", http.StatusBadRequest)
	}

	metricContext := r.URL.Query().Get("context")
	if metricContext == "" {
		pie(logger.Warn, condition_error{"Query parameter `context` is mandatory"}, "", http.StatusBadRequest)
	}


	ret := customMetricOutputReturn{}
	for _, nid := range nodes {
		if timestamps, values, err := h.db.GetMetricData(fmt.Sprintf("%v", job.SlurmId), metricName, metricContext, nid.Nid, cluster); err != nil {
			logger.Error().Msgf("Failed getting custom userdata from database")
			pie(logger.Warn, err, "Failed getting custom data from database", http.StatusBadRequest)
		} else {
			ret[nid.Nid] = append(ret[nid.Nid], customMetricOutput{
				MetricName: metricName,
				Context: metricContext,
				Time: timestamps,
				MetricValue: values,
			})
		}
	}

	custom_metrics_bytes, err := json.Marshal(ret)
	pie(logger.Error, err, "Failed converting data to JSON return value", http.StatusInternalServerError)
	_, _ = w.Write(custom_metrics_bytes)
}

type customMetricInput struct {
	MetricName  string `json:"name"`
	MetricValue string `json:"value"`
	Context     string `json:"context"`
	Xname       string `json:"xname"`
	Timestamp   int64  `json:"timestamp"`
}

func (h customMetric) Post(w http.ResponseWriter, r *http.Request) {
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

	vars := mux.Vars(r)
	if vars["system_name"] == "" {
		pie(logger.Warn, condition_error{"`system_name` must be a valid system"}, "", http.StatusBadRequest)
	}
	if vars["job_id"] == "" {
		pie(logger.Warn, condition_error{"`job_id` must not be empty"}, "", http.StatusBadRequest)
	}
	if vars["node_id"] == "" {
		pie(logger.Warn, condition_error{"`node_id` must not be empty"}, "", http.StatusBadRequest)
	}
	var inData customMetricInput
	err := json.NewDecoder(r.Body).Decode(&inData)
	pie(logger.Warn, err, "", http.StatusBadRequest)

	if inData.MetricName == "" {
		pie(logger.Warn, condition_error{"Field `name` is missing"}, "", http.StatusBadRequest)
	}
	if inData.MetricValue == "" {
		pie(logger.Warn, condition_error{"Field `value` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Context == "" {
		pie(logger.Warn, condition_error{"Field `context` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Xname == "" {
		pie(logger.Warn, condition_error{"Field `xname` is missing"}, "", http.StatusBadRequest)
	}
	if inData.Timestamp == 0 {
		inData.Timestamp = time.Now().Unix()
	}

	if !h.db.PushMetricData(inData.Timestamp, vars["job_id"], inData.MetricName, inData.MetricValue, inData.Xname, vars["node_id"], inData.Context, vars["system_name"]) {
		logger.Error().Msgf("Failed pushing custom userdata to database")
		w.Write([]byte("Failed pushing custom userdata to database"))
	} else {
		w.Write([]byte("ok"))
	}
}
