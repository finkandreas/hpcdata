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

type memory struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetNodeMemoryHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(memory{config, esclient})
}

func (h memory) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch memory data data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	memoryData, err := h.esclient.GetMemoryData(nodes, from, to, logger)
	pie(logger.Error, err, "Failed getting memory data", http.StatusBadRequest)

	type Memory struct {
		Free   []float64 `json:"free"`
		Cache  []float64 `json:"cache"`
		Buffer []float64 `json:"buffer"`
		Unit   string    `json:"memory_unit"`
	}
	ret := struct {
		Time  []epochTime       `json:"time"`
		Nodes map[string]Memory `json:"nodes"`
	}{as_epoch_array(memoryData.Time), map[string]Memory{}}
	for nid, md := range memoryData.MemoryByNode {
		ret.Nodes[nid] = Memory{Free: md.Free, Cache: md.Cache, Buffer: md.Buffer, Unit: "kilobytes"}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
