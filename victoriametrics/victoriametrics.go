package victoriametrics

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

const wanted_num_timestamps = 5000

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

	info, ok := SupportedDCGMMetrics[metricName]
	if !ok {
		return nil, fmt.Errorf("Unsupported DCGM metric: %v", metricName)
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
	for i := range numPoints {
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
				logger.Warn().Msgf("Failed parsing timestamp %v as float64 from type %T, skipping sample", pair[0], pair[0])
				continue
			}
			valStr, ok := pair[1].(string)
			if !ok {
				logger.Warn().Msgf("Failed parsing value %v as string from type %T, skipping sample", pair[1], pair[1])
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
			} else {
				logger.Warn().Msgf("Time index %v falling out of bounds [%v, %v)", idx, 0, numPoints)
			}
		}
	}

	return &ret, nil
}

type vmstatMetric struct {
	Time         []time.Time
	MetricByNode map[string][]float64
}

func (c *Client) GetVmstatMetric(metricName string, nodes []util.Node, from time.Time, to time.Time, vcluster string, logger *zerolog.Logger) (*vmstatMetric, error) {
	if logger == nil {
		logger = logging.Get()
	}

	info, ok := SupportedVmstatMetrics[metricName]
	if !ok {
		return nil, fmt.Errorf("Unsupported vmstat metric: %v", metricName)
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

	ret := vmstatMetric{MetricByNode: map[string][]float64{}}

	if len(resp.Data.Result) == 0 {
		return &ret, nil
	}

	// Build a time grid aligned to the requested step
	numPoints := int(to.Sub(from).Seconds()/stepDuration.Seconds()) + 1
	ret.Time = make([]time.Time, numPoints)
	for i := range numPoints {
		ret.Time[i] = from.Add(time.Duration(i) * stepDuration)
	}

	for _, series := range resp.Data.Result {
		hostname := series.Metric["hostname"]

		ret.MetricByNode[hostname] = make([]float64, numPoints)

		for _, pair := range series.Values {
			if len(pair) != 2 {
				continue
			}
			tsFloat, ok := pair[0].(float64)
			if !ok {
				logger.Warn().Msgf("Failed parsing timestamp %v as float64 from type %T, skipping sample", pair[0], pair[0])
				continue
			}
			valStr, ok := pair[1].(string)
			if !ok {
				logger.Warn().Msgf("Failed parsing value %v as string from type %T, skipping sample", pair[1], pair[1])
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
				ret.MetricByNode[hostname][idx] = val
			} else {
				logger.Warn().Msgf("Time index %v falling out of bounds [%v, %v)", idx, 0, numPoints)
			}
		}
	}

	return &ret, nil
}

func (c *Client) GetChassisEnergy(nodes []util.Node, from time.Time, to time.Time, vcluster string, logger *zerolog.Logger) (*elastic.ChassisEnergy, error) {
	if logger == nil {
		logger = logging.Get()
	}

	info := SupportedChassisEnergyMetric
	step := getStep(from, to, 5*time.Second)
	query := buildChassisEnergyQuery(info.VMMetric, vcluster, nodes)

	logger.Debug().Msgf("VictoriaMetrics metric=%v query=%v start=%v end=%v step=%v", info.VMMetric, query, from.Unix(), to.Unix(), step)

	resp, err := c.queryRange(query, from, to, step, logger)
	if err != nil {
		return nil, fmt.Errorf("Failed querying VictoriaMetrics for chassis energy: %w", err)
	}

	stepDuration, err := time.ParseDuration(step)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing step duration %v: %w", step, err)
	}

	ret := elastic.ChassisEnergy{EnergyByNode: map[string][]float64{}}

	if len(resp.Data.Result) == 0 {
		return &ret, nil
	}

	numPoints := int(to.Sub(from).Seconds()/stepDuration.Seconds()) + 1
	ret.Time = make([]time.Time, numPoints)
	for i := range numPoints {
		ret.Time[i] = from.Add(time.Duration(i) * stepDuration)
	}

	// a node may report its energy counter under VoltageRegulator/Input,
	// under CPUSubsystem/Output, or under both labels. Keep exactly one
	// series per node, with VoltageRegulator/Input taking precedence, so
	// that both series of a dual-reporting node never mix and never overwrite
	// each other's grid slots.
	type vmSeries struct {
		metric map[string]string
		values [][]interface{}
	}
	seriesByNode := map[string]vmSeries{} // key: node_id ("nid006033")
	priority := map[string]int{
		"VoltageRegulator": 0,
		"CPUSubsystem":     1,
	}
	for _, series := range resp.Data.Result {
		nid := series.Metric["nid"]
		if nid == "" {
			logger.Warn().Msgf("Energy series without nid label, skipping: %v", series.Metric)
			continue
		}
		node_id := "nid" + strings.Repeat("0", 6-len(nid)) + nid
		candidate := vmSeries{metric: series.Metric, values: series.Values}
		current, exists := seriesByNode[node_id]
		if !exists {
			seriesByNode[node_id] = candidate
			continue
		}
		pNew, okNew := priority[candidate.metric["physical_context"]]
		pCur, okCur := priority[current.metric["physical_context"]]
		if !okNew || !okCur {
			// unknown physical_context: keep lexicographic order for determinism
			if candidate.metric["physical_context"] > current.metric["physical_context"] {
				seriesByNode[node_id] = candidate
			}
			continue
		}
		if pNew < pCur {
			seriesByNode[node_id] = candidate
		}
	}
	if len(seriesByNode) == 0 {
		return &ret, nil
	}

	for node_id, series := range seriesByNode {
		ret.EnergyByNode[node_id] = make([]float64, numPoints)

		var firstValue float64
		doshift := false // subtract initial value
		lastKnown := -1  // index of the last grid slot which received a data sample

		for _, pair := range series.values {
			if len(pair) != 2 {
				continue
			}
			tsFloat, ok := pair[0].(float64)
			if !ok {
				logger.Warn().Msgf("Failed parsing timestamp %v as float64 from type %T, skipping sample", pair[0], pair[0])
				continue
			}
			valStr, ok := pair[1].(string)
			if !ok {
				logger.Warn().Msgf("Failed parsing value %v as string from type %T, skipping sample", pair[1], pair[1])
				continue
			}
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				logger.Warn().Msgf("Failed parsing value %v as float64, skipping sample", valStr)
				continue
			}
			ts := time.Unix(int64(tsFloat), 0)
			if ts.Before(from) {
				// outside the requested time window
				continue
			}
			idx := int(ts.Sub(from).Seconds() / stepDuration.Seconds())
			if idx >= 0 && idx < numPoints {
				if doshift {
					val -= firstValue
				} else {
					firstValue = val
					doshift = true
					val = 0
				}
				ret.EnergyByNode[node_id][idx] = val

				if ret.EnergyByNode[node_id][idx] != 0 || lastKnown == -1 {
					lastKnown = idx
				}
			} else if idx >= numPoints {
				logger.Warn().Msgf("Time index %v falling out of bounds [%v, %v)", idx, 0, numPoints)
			}
		}

		// Forward-fill holes (missing samples) with the last known value
		if lastKnown >= 0 {
			last := ret.EnergyByNode[node_id][lastKnown]
			for i := lastKnown + 1; i < numPoints; i++ {
				ret.EnergyByNode[node_id][i] = last
			}
		}
	}

	return &ret, nil
}

func (c *Client) GetChassisPower(nodes []util.Node, from time.Time, to time.Time, vcluster string, logger *zerolog.Logger) (*elastic.ChassisPower, error) {
	if logger == nil {
		logger = logging.Get()
	}

	info := SupportedChassisPowerMetric
	step := getStep(from, to, 10*time.Second)
	query := buildChassisPowerQuery(info.VMMetric, vcluster, nodes)

	logger.Debug().Msgf("VictoriaMetrics metric=%v query=%v start=%v end=%v step=%v", info.VMMetric, query, from.Unix(), to.Unix(), step)

	resp, err := c.queryRange(query, from, to, step, logger)
	if err != nil {
		return nil, fmt.Errorf("Failed querying VictoriaMetrics for chassis power: %w", err)
	}

	stepDuration, err := time.ParseDuration(step)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing step duration %v: %w", step, err)
	}

	ret := elastic.ChassisPower{PowerByNode: map[string][]float64{}}

	if len(resp.Data.Result) == 0 {
		return &ret, nil
	}

	numPoints := int(to.Sub(from).Seconds()/stepDuration.Seconds()) + 1
	ret.Time = make([]time.Time, numPoints)
	for i := range numPoints {
		ret.Time[i] = from.Add(time.Duration(i) * stepDuration)
	}

	for _, series := range resp.Data.Result {
		nid := series.Metric["nid"]
		node_id := "nid" + strings.Repeat("0", 6-len(nid)) + nid
		ret.PowerByNode[node_id] = make([]float64, numPoints)
		lastKnown := -1 // index of the last grid slot which received a data sample

		for _, pair := range series.Values {
			if len(pair) != 2 {
				continue
			}
			tsFloat, ok := pair[0].(float64)
			if !ok {
				logger.Warn().Msgf("Failed parsing timestamp %v as float64 from type %T, skipping sample", pair[0], pair[0])
				continue
			}
			valStr, ok := pair[1].(string)
			if !ok {
				logger.Warn().Msgf("Failed parsing value %v as string from type %T, skipping sample", pair[1], pair[1])
				continue
			}
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				logger.Warn().Msgf("Failed parsing value %v as float64, skipping sample", valStr)
				continue
			}
			ts := time.Unix(int64(tsFloat), 0)
			if ts.Before(from) {
				// outside the requested time window
				continue
			}
			idx := int(ts.Sub(from).Seconds() / stepDuration.Seconds())
			if idx >= 0 && idx < numPoints {
				ret.PowerByNode[node_id][idx] = val
				lastKnown = idx
			} else if idx >= numPoints {
				logger.Warn().Msgf("Time index %v falling out of bounds [%v, %v)", idx, 0, numPoints)
			}
		}

		// Forward-fill holes (missing samples) with the last known value
		if lastKnown >= 0 {
			last := ret.PowerByNode[node_id][lastKnown]
			for i := lastKnown + 1; i < numPoints; i++ {
				ret.PowerByNode[node_id][i] = last
			}
		}
	}

	return &ret, nil
}

func (c *Client) queryRange(query string, from time.Time, to time.Time, step string, logger *zerolog.Logger) (*queryResponse, error) {
	if logger == nil {
		logger = logging.Get()
	}

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(from.Unix(), 10))
	params.Set("end", strconv.FormatInt(to.Unix(), 10))
	params.Set("step", step)

	requestURL := fmt.Sprintf("%v/select/0/prometheus/api/v1/query_range?%v", c.baseURL, params.Encode())
	logger.Debug().Msgf("VictoriaMetrics query_range URL=%v", requestURL)

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

	// log the entire response [!! only for debugging !!]
	//logger.Debug().Msgf("VictoriaMetrics query_range response: %v", string(resp.ResponseData))

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
	regex := strings.Join(hostnames, "|")
	return fmt.Sprintf(`%v{vcluster="%v", hostname=~"%v"}`, metric, vcluster, regex)
}

// build a PromQL query for chassis telemetry energy metrics, filtering by vcluster
// and by a nid regex
//
// Allow for missing VoltageRegulator/Input, in which case CPUSubsystem/Output is fetched
// If both are available, VoltageRegulator/Input is fetched
func buildChassisEnergyQuery(metric string, vcluster string, nodes []util.Node) string {
	nidregex := buildNidRegex(nodes)
	return fmt.Sprintf(
		`%v{vcluster="%v",nid=~"%v",parental_context="Chassis",`+
			`physical_context="VoltageRegulator",physical_subcontext="Input"}`+
			` or on(nid) %v{vcluster="%v",nid=~"%v",parental_context="Chassis",`+
			`physical_context="CPUSubsystem",physical_subcontext="Output"}`,
		metric, vcluster, nidregex, metric, vcluster, nidregex)
}

// build a PromQL query for chassis telemetry power metrics, filtering by vcluster
// and by a nid regex
//
// Index 0 of Chassis/VoltageRegulator/Input is the node total input power
func buildChassisPowerQuery(metric string, vcluster string, nodes []util.Node) string {
	regex := buildNidRegex(nodes)
	// reproduce elastic query: average over all six VR input indices (this is NOT total node power)
	// return fmt.Sprintf(
	// 	`avg by (nid) (%v{vcluster="%v",nid=~"%v",parental_context="Chassis",`+
	// 		`physical_context="VoltageRegulator",physical_subcontext="Input"})`,
	// 	metric, vcluster, regex)

	// Correct query for total node power
	return fmt.Sprintf(
		`%v{vcluster="%v",nid=~"%v",parental_context="Chassis",`+
			`physical_context="VoltageRegulator",physical_subcontext="Input",index="0"}`,
		metric, vcluster, regex)
}

// convert a list of nodes (in the format nid006318) into a single regex alternation string (for instance "6318|5078|5079|5100")
func buildNidRegex(nodes []util.Node) string {
	nids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		nid := strings.TrimLeft(strings.TrimPrefix(n.Nid, "nid"), "0")
		nids = append(nids, regexp.QuoteMeta(nid))
	}
	return strings.Join(nids, "|")
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
