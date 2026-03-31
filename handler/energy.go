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

type chassisEnergy struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetChassisEnergyHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(chassisEnergy{config, esclient})
}

func (h chassisEnergy) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch chassis energy data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	chassisEnergy, err := h.esclient.GetChassisEnergy(nodes, from, to, logger)
	pie(logger.Error, err, "Failed getting chassis energy", http.StatusInternalServerError)

	type ChassisEnergy struct {
		Energy []float64 `json:"energy"`
		Unit   string    `json:"energy_unit"`
	}
	ret := struct {
		Time  []epochTime              `json:"time"`
		Nodes map[string]ChassisEnergy `json:"nodes"`
	}{as_epoch_array(chassisEnergy.Time), map[string]ChassisEnergy{}}
	for nid, energy := range chassisEnergy.EnergyByNode {
		ret.Nodes[nid] = ChassisEnergy{Energy: energy, Unit: "Joules"}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
