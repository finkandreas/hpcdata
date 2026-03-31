package handler

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/gorilla/mux"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type chassisPower struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetChassisPowerHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(chassisPower{config, esclient})
}

func (h chassisPower) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch chassis power data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	chassisPower, err := h.esclient.GetChassisPower(nodes, from, to, logger)
	pie(logger.Error, err, "Failed getting chassis power", http.StatusInternalServerError)

	type ChassisPower struct {
		Power []float64 `json:"power"`
		Unit  string    `json:"power_unit"`
	}
	ret := struct {
		Time  []epochTime             `json:"time"`
		Nodes map[string]ChassisPower `json:"nodes"`
	}{as_epoch_array(chassisPower.Time), map[string]ChassisPower{}}
	for nid, power := range chassisPower.PowerByNode {
		ret.Nodes[nid] = ChassisPower{Power: power, Unit: "Watt"}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
