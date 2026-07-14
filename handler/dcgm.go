package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	"github.com/gorilla/mux"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type dcgm struct {
	config   *util.Config
	esclient *elastic.Client
	metric   string
}

type dcgmReturn struct {
	Name string
	Unit string
}

var dcgmMetricUnit = map[string]dcgmReturn{
	"gpu_temp":        {"temperature", "°C"},
	"gpu_utilization": {"utilization", "%"},
}

func GetDcgmData(config *util.Config, esclient *elastic.Client, metric string) func(w http.ResponseWriter, r *http.Request) {
	return wrap(dcgm{config, esclient, metric})
}

func (h dcgm) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch dcgm data data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	dcgmData, err := h.esclient.GetDcgmData(nodes, from, to, h.metric, logger)
	pie(logger.Error, err, "Failed getting DCGM data", http.StatusBadRequest)

	ret := struct {
		Time  []epochTime               `json:"time"`
		Nodes map[string]map[string]any `json:"nodes"`
	}{as_epoch_array(dcgmData.Time), map[string]map[string]any{}}
	for nid, dcgmMetric := range dcgmData.MetricByNode {
		ret.Nodes[nid] = map[string]any{h.metric: dcgmMetric, fmt.Sprintf("%v_unit", h.metric): dcgmMetricUnit[h.metric]}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
