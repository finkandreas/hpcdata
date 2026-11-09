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
	"cscs.ch/hpcdata/victoriametrics"
)

type vmVmstat struct {
	config   *util.Config
	esclient *elastic.Client
	vmclient *victoriametrics.Client
}

func GetVMVmstatHandler(config *util.Config, esclient *elastic.Client, vmclient *victoriametrics.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(vmVmstat{config, esclient, vmclient})
}

func (h vmVmstat) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	vars := mux.Vars(r)
	systemName := vars["system_name"]
	metricName := vars["metric_name"]

	info, ok := victoriametrics.SupportedVmstatMetrics[metricName]
	if !ok {
		pie(logger.Warn, condition_error{fmt.Sprintf("Unsupported vmstat metric: %v", metricName)}, "", http.StatusBadRequest)
	}

	logger.Debug().Msgf("Passed all security checks to fetch VM vmstat metric=%v for job=%+v in the time window from=%v to=%v", metricName, job, from, to)

	cluster_config, err := h.config.GetClusterConfig(systemName)
	pie(logger.Warn, err, "", http.StatusBadRequest)

	// [TO DO] 8 lines below are taken from dcgm.go ---> refactor to avoid duplicate
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}

	vmstatData, err := h.vmclient.GetVmstatMetric(metricName, nodes, from, to, cluster_config.Name, logger)
	pie(logger.Error, err, fmt.Sprintf("Failed getting vmstat metric %v", metricName), http.StatusInternalServerError)

	ret := struct {
		Time  []epochTime               `json:"time"`
		Nodes map[string]map[string]any `json:"nodes"`
	}{as_epoch_array(vmstatData.Time), map[string]map[string]any{}}
	for nid, vmstatMetric := range vmstatData.MetricByNode {
		ret.Nodes[nid] = map[string]any{metricName: vmstatMetric, fmt.Sprintf("%v_unit", metricName): info.Unit}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
