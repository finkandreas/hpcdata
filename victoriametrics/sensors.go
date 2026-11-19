package victoriametrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

// one (nid, index) time series
type SensorSeries struct {
	Index int       `json:"index"`
	Data  []float64 `json:"data"`
}

type SensorData struct {
	Time  []time.Time
	Unit  string
	Nodes map[string][]SensorSeries // key==nodeid (e.g. "nid001234")
}

type sensorKind struct {
	metric   string
	selector string // additional label matchers, comma-prefixed, e.g. `,physical_context="GPU"`
	unit     string
	energy   bool // true for an energy data, which is cumulative
	minStep  time.Duration
}

var supportedSensorKinds = map[string]sensorKind{
	"node_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,parental_context="Chassis",physical_context="VoltageRegulator",physical_subcontext="Input",index="0"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
	"node_energy": {
		metric:   SupportedChassisEnergyMetric.VMMetric,
		selector: `,parental_context="Chassis",physical_context="VoltageRegulator",physical_subcontext="Input",index="0"`,
		unit:     "J",
		energy:   true,
		minStep:  5 * time.Second,
	},
	"gpu_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,physical_context="GPU"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
	"gpu_module_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,physical_context="GPUModule"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
	"cpu_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,physical_context="CPU"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
	"cpu_subsystem_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,parental_context="Chassis",physical_context="CPUSubsystem",physical_subcontext="Output"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
	"cpu_subsystem_energy": {
		metric:   SupportedChassisEnergyMetric.VMMetric,
		selector: `,parental_context="Chassis",physical_context="CPUSubsystem",physical_subcontext="Output"`,
		unit:     "J",
		energy:   true,
		minStep:  5 * time.Second,
	},
	"aux_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,parental_context="Chassis",physical_context="VoltageRegulator",physical_subcontext="Input",index="5"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
	"vr_output_node_power": {
		metric:   SupportedChassisPowerMetric.VMMetric,
		selector: `,parental_context="Chassis",physical_context="VoltageRegulator",physical_subcontext="Output",index="0"`,
		unit:     "W",
		minStep:  5 * time.Second,
	},
}

func SensorKindExists(kind string) bool {
	_, ok := supportedSensorKinds[kind]
	return ok
}

// build a PromQL query for per-sensor (indexed) telemetry metrics, filtering
// by vcluster, by a nid regex and by the kind's additional label selector
func buildSensorQuery(metric string, vcluster string, nodes []util.Node, selector string) string {
	regex := buildNidRegex(nodes)
	return fmt.Sprintf(`%v{vcluster="%v",nid=~"%v"%v}`, metric, vcluster, regex, selector)
}

func (c *Client) GetSensorData(kind string, nodes []util.Node, from time.Time, to time.Time, vcluster string, logger *zerolog.Logger) (*SensorData, error) {
	sensorKindInfo, ok := supportedSensorKinds[kind]
	if !ok {
		names := make([]string, 0, len(supportedSensorKinds))
		for name := range supportedSensorKinds {
			names = append(names, name)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("Unsupported sensor kind: %q, supported kinds: %v", kind, strings.Join(names, ", "))
	}
	return c.getSensorSeries(sensorKindInfo, nodes, from, to, vcluster, logger)
}

func (c *Client) getSensorSeries(kind sensorKind, nodes []util.Node, from time.Time, to time.Time, vcluster string, logger *zerolog.Logger) (*SensorData, error) {
	if logger == nil {
		logger = logging.Get()
	}

	step := getStep(from, to, kind.minStep)
	query := buildSensorQuery(kind.metric, vcluster, nodes, kind.selector)

	logger.Debug().Msgf("VictoriaMetrics metric=%v query=%v start=%v end=%v step=%v", kind.metric, query, from.Unix(), to.Unix(), step)

	resp, err := c.queryRange(query, from, to, step, logger)
	if err != nil {
		return nil, fmt.Errorf("Failed querying VictoriaMetrics for metric %v: %w", kind.metric, err)
	}

	stepDuration, err := time.ParseDuration(step)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing step duration %v: %w", step, err)
	}

	ret := SensorData{Unit: kind.unit, Nodes: map[string][]SensorSeries{}}

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

		indexStr, ok := series.Metric["index"]
		if !ok {
			logger.Warn().Msgf("Series without index label, skipping: %v", series.Metric)
			continue
		}
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			logger.Warn().Msgf("Failed parsing index=%v as integer, skipping series: %v", indexStr, series.Metric)
			continue
		}

		// find or create the slot for this (nid, index) pair
		slots := ret.Nodes[node_id]
		slot := -1
		for i, s := range slots {
			if s.Index == index {
				slot = i
				break
			}
		}
		if slot == -1 {
			ret.Nodes[node_id] = append(ret.Nodes[node_id], SensorSeries{Index: index, Data: make([]float64, numPoints)})
			slot = len(ret.Nodes[node_id]) - 1
		}

		firstVal := 0.0
		doshift := false // subtract initial value
		lastKnown := -1  // index of the last grid slot which received a data sample

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
				if kind.energy {
					if doshift {
						val -= firstVal
					} else {
						firstVal = val
						doshift = true
						val = 0
					}
				}
				ret.Nodes[node_id][slot].Data[idx] = val
				if val != 0 || lastKnown == -1 {
					lastKnown = idx
				}
			} else if idx >= numPoints {
				logger.Warn().Msgf("Time index %v falling out of bounds [%v, %v)", idx, 0, numPoints)
			}
		}

		// Forward-fill holes (missing samples) with the last known value
		if lastKnown >= 0 {
			last := ret.Nodes[node_id][slot].Data[lastKnown]
			for i := lastKnown + 1; i < numPoints; i++ {
				ret.Nodes[node_id][slot].Data[i] = last
			}
		}
	}

	// deterministic ordering: series sorted by index within each node
	for node_id := range ret.Nodes {
		slots := ret.Nodes[node_id]
		sort.Slice(slots, func(i, j int) bool { return slots[i].Index < slots[j].Index })
	}

	return &ret, nil
}
