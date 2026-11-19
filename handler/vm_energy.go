package handler

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/gorilla/mux"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
	"cscs.ch/hpcdata/victoriametrics"
)

type vmChassisEnergy struct {
	config   *util.Config
	esclient *elastic.Client
	vmclient *victoriametrics.Client
}

func GetVMChassisEnergyHandler(config *util.Config, esclient *elastic.Client, vmclient *victoriametrics.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(vmChassisEnergy{config, esclient, vmclient})
}

func (h vmChassisEnergy) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch VictoriaMetrics chassis energy data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	systemName := vars["system_name"]

	cluster_config, err := h.config.GetClusterConfig(systemName)
	pie(logger.Warn, err, "", http.StatusBadRequest)

	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}

	chassisEnergy, err := h.vmclient.GetChassisEnergy(nodes, from, to, cluster_config.Name, logger)
	pie(logger.Error, err, "Failed getting VictoriaMetrics chassis energy", http.StatusInternalServerError)

	type ChassisEnergy struct {
		Energy []float64 `json:"energy"`
		Unit   string    `json:"energy_unit"`
	}
	ret := struct {
		Time  []epochTime              `json:"time"`
		Nodes map[string]ChassisEnergy `json:"nodes"`
	}{as_epoch_array(chassisEnergy.Time), map[string]ChassisEnergy{}}
	for nid, energy := range chassisEnergy.EnergyByNode {
		ret.Nodes[nid] = ChassisEnergy{Energy: energy, Unit: "Joule"}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
