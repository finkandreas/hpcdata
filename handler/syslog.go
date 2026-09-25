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

type syslog struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetSyslogHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(syslog{config, esclient})
}

func (h syslog) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch syslog logs for job=%+v in the time window from=%v to=%v", job, from, to)

	vars := mux.Vars(r)
	nodes := job.Nodes
	if node_id, exists := vars["node_id"]; exists {
		nodes = []util.Node{{Nid: node_id}}
		// security check that the node is part of the job
		if !slices.ContainsFunc(job.Nodes, func(n util.Node) bool { return n.Nid == node_id }) {
			pie(logger.Warn, condition_error{"The requested node_id is not part of the job"}, "", http.StatusBadRequest)
		}
	}
	syslogData, err := h.esclient.GetSyslog(nodes, from, to, logger)
	pie(logger.Error, err, "Failed getting syslog data", http.StatusBadRequest)

	type syslogByNode struct {
		Time          []epochTime               `json:"time"`
		SyslogEntries []elastic.SyslogDataEntry `json:"syslog"`
	}
	type syslogRet map[string]syslogByNode

	ret := syslogRet{}
	for nid, nidSyslogData := range *syslogData {
		ret[nid] = syslogByNode{as_epoch_array(nidSyslogData.Time), nidSyslogData.Syslog}
	}
	write_bytes, err := json.Marshal(ret)
	pie(logger.Error, err, "Failed marshaling value to json", http.StatusInternalServerError)
	_, _ = w.Write(write_bytes)
}
