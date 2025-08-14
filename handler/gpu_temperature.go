package handler;

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/gorilla/mux"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type gpuTemperature struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetGpuTemperatureHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(gpuTemperature{config, esclient})
}

func (h gpuTemperature) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch GPU temperature data for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if ! slices.ContainsFunc(job.Nodes, func(n util.Node) bool{ return n.Nid==node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	gpuTemp, err := h.esclient.GetGpuTemperature(nodes, from, to, logger)
	pie(logger.Error, err, "Failed getting GPU temperatures", http.StatusInternalServerError)


	type NodesTemperatures struct {
		GpuIndex int `json:"gpu_id"`
		Temperatures []float64 `json:"temperatures"`
		Unit string `json:"temperatures_unit"`
	}
	ret := struct {
		Time []epochTime `json:"time"`
		Nodes map[string][]NodesTemperatures `json:"nodes"`
	} {as_epoch_array(gpuTemp.Time), map[string][]NodesTemperatures{}}
	for k,v := range gpuTemp.Temperatures {
		for _, temps := range v{
			ret.Nodes[k] = append(ret.Nodes[k], NodesTemperatures{GpuIndex: temps.GpuIndex, Temperatures: temps.Temperatures, Unit: "°C"})
		}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
