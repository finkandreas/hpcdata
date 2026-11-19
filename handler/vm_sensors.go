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

type vmSensor struct {
	config   *util.Config
	esclient *elastic.Client
	vmclient *victoriametrics.Client
}

func GetVMSensorHandler(config *util.Config, esclient *elastic.Client, vmclient *victoriametrics.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(vmSensor{config, esclient, vmclient})
}

func (h vmSensor) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	vars := mux.Vars(r)
	kind := vars["kind"]
	if !victoriametrics.SensorKindExists(kind) {
		pie(logger.Warn, herr("Unknown energypower kind: "+kind, "kind=`"+kind+"`"), "", http.StatusBadRequest)
	}

	logger.Debug().Msgf("Passed all security checks to fetch VictoriaMetrics sensor data kind=%v for job=%+v in the time window from=%v to=%v", kind, job, from, to)

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

	sensorData, err := h.vmclient.GetSensorData(kind, nodes, from, to, cluster_config.Name, logger)
	pie(logger.Error, err, "Failed getting VictoriaMetrics sensor data", http.StatusInternalServerError)

	type SensorNode struct {
		Unit   string                         `json:"unit"`
		Series []victoriametrics.SensorSeries `json:"series"`
	}
	ret := struct {
		Time  []epochTime           `json:"time"`
		Nodes map[string]SensorNode `json:"nodes"`
	}{as_epoch_array(sensorData.Time), make(map[string]SensorNode, len(nodes))}
	for _, n := range nodes {
		ret.Nodes[n.Nid] = SensorNode{Unit: sensorData.Unit, Series: []victoriametrics.SensorSeries{}}
	}
	for nid, series := range sensorData.Nodes {
		ret.Nodes[nid] = SensorNode{Unit: sensorData.Unit, Series: series}
	}

	write_bytes, err := json.Marshal(ret)
	_, _ = w.Write(write_bytes)
}
