package handler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/firecrest"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type capstorGlobal struct {
	config   *util.Config
	esclient *elastic.Client
}

func GetCapstorGlobalHandler(config *util.Config, esclient *elastic.Client) func(w http.ResponseWriter, r *http.Request) {
	return wrap(capstorGlobal{config, esclient})
}

func get_job(jobid string, cluster_config *util.ClusterConfig, f7t_client *firecrest.Client, esclient *elastic.Client, config *util.Config, logger *zerolog.Logger) (*util.Job, error) {
	if job, err := get_job_via_f7t(jobid, f7t_client, logger); err != nil {
		logger.Warn().Msgf("Failed getting job via firecrest. err=%v", err)
		if job, err := esclient.GetJob(jobid, cluster_config.ElasticName, logger); err != nil {
			return nil, err
		} else {
			return job, nil
		}
	} else {
		return job, nil
	}
}

func get_job_via_f7t(jobid string, f7t_client *firecrest.Client, logger *zerolog.Logger) (*util.Job, error) {
	if f7t_job, err := f7t_client.Job(jobid); err != nil {
		return nil, err
	} else {
		logger.Debug().Msgf("Successfully fetched job via firecrest. Job=%#v", f7t_job)
		submit_account, _ := strings.CutPrefix(f7t_job.Account, "a-")
		ret := util.Job{
			SlurmId: f7t_job.JobId,
			Account: submit_account,
			Start:   time.Unix(int64(f7t_job.Time.Start), 0),
		}
		if f7t_job.Time.End == 0 {
			ret.End = time.Now()
		} else {
			ret.End = time.Unix(int64(f7t_job.Time.End), 0)
		}
		ret.Nodes = util.ExpandNodes(f7t_job.Nodes)
		return &ret, nil
	}
}

/*
	Returns

	{
		"time": [int] <epoch-time>,
		"read_bandwidth": []float <average bytes/sec>,
		"read_iops": []float <average ops/sec>,
		"write_bandwidth": []float <average bytes/sec>,
		"write_iops": []float <average ops/sec>,
		"metadata_ops": []float <average ops/sec>,
		"nodes_loadavg": [][5]int <number of nodes with 1-min system loadavg [0,20), [20,40), [40,80), [80,160), [160, inf)>,
	}
*/
func (h capstorGlobal) Get(w http.ResponseWriter, r *http.Request) {
	logger := logging.GetReqLogger(r)
	job, from, to := panic_if_no_access(r, h.esclient, h.config)

	logger.Debug().Msgf("Passed all security checks to fetch capstor global data for job=%+v in the time window from=%v to=%v", job, from, to)

	fsstats, err := h.esclient.GetGlobalFilesystem(elastic.Capstor, from, to, logger)
	pie(logger.Error, err, "Failed getting filesystem stats", http.StatusInternalServerError)

	const unitBw = "Average bytes/s"
	const unitIops = "Average number operations/s"
	const unitLoad = "Number of OSS with a 1-min loadavg [[0,20), [20,40), [40,60), [60,80), [80,inf)]"

	ret := struct {
		Time            []epochTime `json:"time"`
		ReadBytes       []float64   `json:"read_bandwidth"`
		ReadBytesUnit   string      `json:"read_bandwidth_unit"`
		ReadIOPS        []float64   `json:"read_iops"`
		ReadIOPSUnit    string      `json:"read_iops_unit"`
		WriteBytes      []float64   `json:"write_bandwidth"`
		WriteBytesUnit  string      `json:"write_bandwidth_unit"`
		WriteIOPS       []float64   `json:"write_iops"`
		WriteIOPSUnit   string      `json:"write_iops_unit"`
		MetadataOPS     []float64   `json:"metadata_ops"`
		MetadataOPSUnit string      `json:"metadata_ops_unit"`
		Load            [][5]int64  `json:"nodes_loadavg"`
		LoadUnit        string      `json:"nodes_loadavg_unit"`
	}{as_epoch_array(fsstats.Time), fsstats.ReadBytes, unitBw, fsstats.ReadIOPS, unitIops, fsstats.WriteBytes, unitBw, fsstats.WriteIOPS, unitIops, fsstats.MetadataOPS, unitIops, fsstats.Load, unitLoad}

	fsstats_bytes, err := json.Marshal(ret)
	_, _ = w.Write(fsstats_bytes)
}
