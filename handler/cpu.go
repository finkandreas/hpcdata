package handler

import (
	"encoding/json"
	"net/http"
	"slices"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
	"github.com/gorilla/mux"
)

type cpu struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetNodeCpuHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(cpu{config, esclient})
}

func (h cpu) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch cpu data data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	cpuData, err := h.esclient.GetCpuData(nodes, from, to, logger)
	pie(logger.Error, err, "Failed getting cpu data", http.StatusBadRequest)

	type Cpu struct {
		User   []float64 `json:"user"`
		System []float64 `json:"system"`
		Unit   string    `json:"cpu_unit"`
	}
	ret := struct {
		Time  []epochTime    `json:"time"`
		Nodes map[string]Cpu `json:"nodes"`
	}{as_epoch_array(cpuData.Time), map[string]Cpu{}}
	for nid, md := range cpuData.CpuByNode {
		ret.Nodes[nid] = Cpu{User: md.User, System: md.System, Unit: "%"}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
