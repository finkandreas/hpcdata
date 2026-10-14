package victoriametrics

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

const wanted_num_timestamps = 5000

type MetricInfo struct {
	VMMetric string // full Prometheus metric name as stored in VictoriaMetrics
	Unit     string // metric unit
}

var SupportedMetrics = map[string]MetricInfo{
	"gpu_temp":        {VMMetric: "cray_storage.dcgm.gpu_temp", Unit: "°C"},
	"memory_temp":     {VMMetric: "cray_storage.dcgm.memory_temp", Unit: "°C"},
	"power_usage":     {VMMetric: "cray_storage.dcgm.power_usage", Unit: "W"},
	"gpu_utilization": {VMMetric: "cray_storage.dcgm.gpu_utilization", Unit: "%"},
}

type Client struct {
	baseURL string
	token   string
}

type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string        `json:"resultType"`
		Result     []querySeries `json:"result"`
	} `json:"data"`
}

type querySeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

func NewClient(config *util.Config) *Client {
	return &Client{
		baseURL: config.VictoriaMetrics.URL,
		token:   config.VictoriaMetrics.Token,
	}
}

func (c *Client) GetDcgmMetric(metricName string, nodes []util.Node, from time.Time, to time.Time, vcluster string, logger *zerolog.Logger) (*elastic.DcgmMetric, error) {
	if logger == nil {
		logger = logging.Get()
	}

	info, ok := SupportedMetrics[metricName]
	if !ok {
		return nil, fmt.Errorf("Unsupported VM DCGM metric: %v", metricName)
	}

	step := getStep(from, to, 10*time.Second) // [TO DO] avoid hard-coding the step duration
	query := buildNodeRegexQuery(info.VMMetric, vcluster, nodes)

	logger.Debug().Msgf("VictoriaMetrics metric=%v query=%v start=%v end=%v step=%v", metricName, query, from.Unix(), to.Unix(), step)

	resp, err := c.queryRange(query, from, to, step, logger)
	if err != nil {
		return nil, fmt.Errorf("Failed querying VictoriaMetrics for metric %v: %w", metricName, err)
	}

	stepDuration, err := time.ParseDuration(step)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing step duration %v: %w", step, err)
	}

	ret := elastic.DcgmMetric{MetricByNode: map[string][]elastic.DcgmDataIndexed{}}

	if len(resp.Data.Result) == 0 {
		return &ret, nil
	}

	// Build a time grid aligned to the requested step
	numPoints := int(to.Sub(from).Seconds()/stepDuration.Seconds()) + 1
	ret.Time = make([]time.Time, numPoints)
	for i := 0; i < numPoints; i++ {
		ret.Time[i] = from.Add(time.Duration(i) * stepDuration)
	}

	for _, series := range resp.Data.Result {
		hostname := series.Metric["hostname"]
		gpuIDStr := series.Metric["gpu_id"]
		gpuID, err := strconv.Atoi(gpuIDStr)
		if err != nil {
			return nil, fmt.Errorf("Failed parsing gpu_id=%v as integer", gpuIDStr)
		}

		// Ensure the per-GPU slot exists and is zero-filled for the whole grid
		for len(ret.MetricByNode[hostname]) <= gpuID {
			ret.MetricByNode[hostname] = append(ret.MetricByNode[hostname], elastic.DcgmDataIndexed{})
			ret.MetricByNode[hostname][len(ret.MetricByNode[hostname])-1].Data = make([]float64, numPoints)
		}
		ret.MetricByNode[hostname][gpuID].GpuIndex = gpuID

		for _, pair := range series.Values {
			if len(pair) != 2 {
				continue
			}
			tsFloat, ok := pair[0].(float64)
			if !ok {
				continue
			}
			valStr, ok := pair[1].(string)
			if !ok {
				continue
			}
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				logger.Warn().Msgf("Failed parsing value %v as float64, skipping sample", valStr)
				continue
			}
			ts := time.Unix(int64(tsFloat), 0)
			idx := int(ts.Sub(from).Seconds() / stepDuration.Seconds())
			if idx >= 0 && idx < numPoints {
				ret.MetricByNode[hostname][gpuID].Data[idx] = val
			}
		}
	}

	return &ret, nil
}

func (c *Client) queryRange(query string, from time.Time, to time.Time, step string, logger *zerolog.Logger) (*queryResponse, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(from.Unix(), 10))
	params.Set("end", strconv.FormatInt(to.Unix(), 10))
	params.Set("step", step)

	requestURL := fmt.Sprintf("%v/select/0/prometheus/api/v1/query_range?%v", c.baseURL, params.Encode())
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %v", c.token),
	}

	resp, err := util.DoRequest("GET", requestURL, headers, nil)
	if err != nil {
		return nil, err
	}
	if err := util.CheckResponse(resp); err != nil {
		return nil, err
	}

	var parsed queryResponse
	if err := json.Unmarshal(resp.ResponseData, &parsed); err != nil {
		return nil, fmt.Errorf("Failed unmarshaling VictoriaMetrics response: %w", err)
	}

	if parsed.Status != "success" {
		return nil, fmt.Errorf("VictoriaMetrics query_range returned non-success status %q for query %q", parsed.Status, query)
	}

	if parsed.Data.ResultType != "matrix" {
		return nil, fmt.Errorf("Unexpected result type different from matrix: %q", parsed.Data.ResultType)
	}

	return &parsed, nil
}

// build a PromQL query that filters by vcluster and by a regex list of hostnames
func buildNodeRegexQuery(metric string, vcluster string, nodes []util.Node) string {
	hostnames := make([]string, 0, len(nodes))
	for _, n := range nodes {
		hostnames = append(hostnames, regexp.QuoteMeta(n.Nid))
	}
	regex := ""
	for i, h := range hostnames {
		if i > 0 {
			regex += "|"
		}
		regex += h
	}
	return fmt.Sprintf("%v{vcluster=\"%v\", hostname=~\"%v\"}", metric, vcluster, regex)
}

// This is a rewrite of elastic.go get_interval
// [TO DO]: avoid duplication
func getStep(from, to time.Time, minStep time.Duration) string {
	totalSec := to.Sub(from).Seconds()
	autoStepSec := math.Ceil(totalSec / wanted_num_timestamps)
	if autoStepSec < minStep.Seconds() {
		return fmt.Sprintf("%vs", math.Ceil(minStep.Seconds()))
	}
	return fmt.Sprintf("%vs", autoStepSec)
}
