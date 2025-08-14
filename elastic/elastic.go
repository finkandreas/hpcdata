package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	es "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/calendarinterval"

	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type Filesystem string
const (
	Capstor Filesystem = "CAPSTOR"
	Iopsstor           = "IOPSSTOR"
)


type Client struct {
	*es.TypedClient
}

func NewClient(config *util.Config) *Client {
	c, err := es.NewTypedClient(es.Config{
		Addresses: []string{config.Elastic.URL},
		Password:  config.Elastic.Password,
		Username:  config.Elastic.Username,
		EnableDebugLogger: true,
		Logger: &elastictransport.CurlLogger{Output: os.Stdout, EnableRequestBody: true, EnableResponseBody: true},
	})
	if err != nil {
		panic("Failed creating ElasticClient")
	}
	return &Client{c}
}

func (c *Client) GetJob(jobid string, cluster_name string, logger *zerolog.Logger) (*util.Job, error) {
	if logger == nil {
		logger = logging.Get()
	}
	res, err := c.Search().
		Index(".ds-logs-slurm.accounting-*").
		Request(&search.Request{
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Filter: []types.Query{
						{Term: map[string]types.TermQuery{"jobid": {Value: jobid}}},
						{Term: map[string]types.TermQuery{"cluster": {Value: cluster_name}}},
					},
				},
			},
		}).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Failed searching in elastic: %w", err)
	}
	logger.Debug().Msgf("Querying job from elastic took %vms. Num results found=%v", res.Took, res.Hits.Total.Value)

	if res.Hits.Total.Value == 0 {
		return nil, fmt.Errorf("No job found - %w", util.ErrInvalidInput)
	}
	if res.Hits.Total.Value > 1 {
		logger.Error().Msgf("Found more than one matching job for cluster=%v and jobid=%v", cluster_name, jobid)
		// do not fail
	}
	type ElasticJob struct {
		Account string `json:"account"`
		JobId   int    `json:"jobid"`
		Start   string `json:"@start"`
		End     string `json:"@end"`
		Nodes   string `json:"nodes"`
	}
	var elasticJob ElasticJob
	logger.Debug().Msgf("elastic job json=%v", string(res.Hits.Hits[len(res.Hits.Hits)-1].Source_))
	err = json.Unmarshal(res.Hits.Hits[len(res.Hits.Hits)-1].Source_, &elasticJob)
	if err != nil {
		return nil, fmt.Errorf("Failed json unpacking of elastic hit: %w", err)
	}

	submission_account, _ := strings.CutPrefix(elasticJob.Account, "a-")
	start, err := time.Parse("2006-01-02T15:04:05", elasticJob.Start)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing start date: %w", err)
	}
	end, err := time.Parse("2006-01-02T15:04:05", elasticJob.End)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing end date: %w", err)
	}

	return &util.Job{SlurmId: elasticJob.JobId, Account: submission_account, Start: start, End: end, Nodes: util.ExpandNodes(elasticJob.Nodes)}, nil
}

type GpuTemperatureIndexed struct {
	GpuIndex int
	Temperatures []float64
}
type GpuTemperatures struct {
	Time []time.Time
	// key==node_id, value = 4 arrays of floats, for the 4 GPUs
	Temperatures map[string][]GpuTemperatureIndexed
}

func (c *Client) GetGpuTemperature(nodes []util.Node, from time.Time, to time.Time, logger *zerolog.Logger) (*GpuTemperatures, error) {
	if logger == nil {
		logger = logging.Get()
	}

	nodesOfInterest := []string{}
	for _, n := range nodes {
		n1, _ := strings.CutPrefix(n.Nid, "nid")
		nodesOfInterest = append(nodesOfInterest, strings.TrimLeft(n1, "0"))
	}

	res, err := c.Search().
		Index(".ds-metrics-facility.telemetry-alps*").
		Request(&search.Request{
			Size: ptr(0), // we are only interested in the aggregation results
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Filter: []types.Query{
						{
							Terms: &types.TermsQuery{TermsQuery: map[string]types.TermsQueryField{"nid": nodesOfInterest}},
						}, {
							Term: map[string]types.TermQuery{"Sensor.PhysicalContext": {Value: "GPU"}},
						}, {
							Term: map[string]types.TermQuery{"MessageId": {Value: "CrayTelemetry.Temperature"}},
						}, {
							Range: map[string]types.RangeQuery{
								"@timestamp": types.DateRangeQuery{
									Format: ptr("epoch_second"),
									Gte: ptr(strconv.FormatInt(from.Unix(), 10)),
									Lt: ptr(strconv.FormatInt(to.Unix(), 10)),
								},
							},
						},
					},
				},
			},
			Aggregations: map[string]types.Aggregations{
				"timestamps": {
					DateHistogram: &types.DateHistogramAggregation{
						Field: ptr("@timestamp"),
						CalendarInterval: &calendarinterval.Minute,
					},
					Aggregations: map[string]types.Aggregations{
						"nodes": {
							Terms: &types.TermsAggregation{
								Size: ptr(2048),
								Field: ptr("nid"),
							},
							Aggregations: map[string]types.Aggregations{
								"gpu_idx": {
									Terms: &types.TermsAggregation{
										Field: ptr("Sensor.Index"),
									},
									Aggregations: map[string]types.Aggregations{
										"temperature": {Max: &types.MaxAggregation{Field: ptr("Sensor.Value")}},
									},
								},
							},
						},
					},
				},
			},
		}).Do(context.Background())

	if err != nil {
		return nil, fmt.Errorf("Failed GPU temperature searching in elastic: %w", err)
	}

	timestampBuckets := res.Aggregations["timestamps"].(*types.DateHistogramAggregate).Buckets.([]types.DateHistogramBucket)
	logger.Debug().Msgf("Querying GPU temperature from elastic took %vms. Num results in aggregation=%v", res.Took, len(timestampBuckets))
	if len(timestampBuckets) > 0 {
		logger.Debug().Msgf("First bucket result: %v", timestampBuckets[0])
	}

	ret := GpuTemperatures{Temperatures: map[string][]GpuTemperatureIndexed{}}
	for _, timestampBucket := range timestampBuckets {
		ret.Time = append(ret.Time, time.Unix(timestampBucket.Key/1000, 0))
		nodeBuckets := timestampBucket.Aggregations["nodes"].(*types.StringTermsAggregate).Buckets.([]types.StringTermsBucket)
		// append to every already known node and every gpuIndex a NaN, such that it will have in the end the same length as the time array
		for _, t := range ret.Temperatures {
			for i := range t {
				t[i].Temperatures = append(t[i].Temperatures, 0)
			}
		}
		for _, nodeBucket := range nodeBuckets {
			node_id := "nid" + strings.Repeat("0", 6-len(nodeBucket.Key.(string))) + nodeBucket.Key.(string)
			gpuBuckets := nodeBucket.Aggregations["gpu_idx"].(*types.LongTermsAggregate).Buckets.([]types.LongTermsBucket)
			for _, gpuBucket := range gpuBuckets {
				gpuIdx := int(gpuBucket.Key)
				// ensure we have the temperatures for gpuIdx available
				// ensure also that it is prefilled with as many NaN as the first  array
				for len(ret.Temperatures[node_id]) <= gpuIdx {
					ret.Temperatures[node_id] = append(ret.Temperatures[node_id], GpuTemperatureIndexed{})
					ret.Temperatures[node_id][len(ret.Temperatures[node_id])-1].Temperatures = make([]float64, len(ret.Time))
					for idx := range(ret.Time) {
						ret.Temperatures[node_id][len(ret.Temperatures[node_id])-1].Temperatures[idx] = 0
					}
				}
				ret.Temperatures[node_id][gpuIdx].GpuIndex = gpuIdx
				ret.Temperatures[node_id][gpuIdx].Temperatures[len(ret.Time)-1] = f64(gpuBucket.Aggregations["temperature"].(*types.MaxAggregate).Value, 0)
			}
		}
	}
	return &ret, nil
}


type FilesystemStats struct {
	Time []time.Time
	ReadBytes []float64
	WriteBytes []float64
	ReadIOPS []float64
	WriteIOPS []float64
	MetadataOPS []float64
	Load [][5]int64
}

func (c *Client) GetGlobalFilesystem(fs Filesystem, from time.Time, to time.Time, logger *zerolog.Logger) (*FilesystemStats, error) {
	if logger == nil {
		logger = logging.Get()
	}
	res, err := c.Search().
		Index(".ds-metrics-legacy.telemetry-clusterstor*").
		Request(&search.Request{
			Size: ptr(0), // we are only interested in the aggregation results
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Filter: []types.Query{
						{
							Term: map[string]types.TermQuery{"System": {Value: fs}},
						}, {
							Range: map[string]types.RangeQuery{
								"@timestamp": types.DateRangeQuery{
									Format: ptr("epoch_second"),
									Gte: ptr(strconv.FormatInt(from.Unix(), 10)),
									Lt: ptr(strconv.FormatInt(to.Unix(), 10)),
								},
							},
						},
					},
					Should: []types.Query{
						{Exists: &types.ExistsQuery{Field: "totops"}},
						{Exists: &types.ExistsQuery{Field: "read_bytes"}},
						{Exists: &types.ExistsQuery{Field: "read_iops"}},
						{Exists: &types.ExistsQuery{Field: "write_bytes"}},
						{Exists: &types.ExistsQuery{Field: "write_iops"}},
						{Exists: &types.ExistsQuery{Field: "load_one"}},
					},
					MinimumShouldMatch: 1,
				},
			},
			Aggregations: map[string]types.Aggregations{
				"timestamps": {
					DateHistogram: &types.DateHistogramAggregation{
						Field: ptr("@timestamp"),
						CalendarInterval: &calendarinterval.Minute,
					},
					Aggregations: map[string]types.Aggregations{
						"metadataops": {Sum: &types.SumAggregation{Field: ptr("totops")}},
						"read_bytes": {Sum: &types.SumAggregation{Field: ptr("read_bytes")}},
						"read_iops": {Sum: &types.SumAggregation{Field: ptr("read_iops")}},
						"write_bytes": {Sum: &types.SumAggregation{Field: ptr("write_bytes")}},
						"write_iops": {Sum: &types.SumAggregation{Field: ptr("write_iops")}},
						"load_one": {
							Range: &types.RangeAggregation{
								Field: ptr("load_one"),
								Ranges: []types.AggregationRange{
									{To: ptr(types.Float64(20)), Key: ptr("20")},
									{From: ptr(types.Float64(20)), To: ptr(types.Float64(40)), Key: ptr("40")},
									{From: ptr(types.Float64(40)), To: ptr(types.Float64(80)), Key: ptr("80")},
									{From: ptr(types.Float64(80)), To: ptr(types.Float64(160)), Key: ptr("160")},
									{From: ptr(types.Float64(160)), Key: ptr("High")},
								},
							},
						},
					},
				},
			},
		}).Do(context.Background())

	if err != nil {
		return nil, fmt.Errorf("Failed filesystem stats searching in elastic: %w", err)
	}
	aggregateBuckets := res.Aggregations["timestamps"].(*types.DateHistogramAggregate).Buckets.([]types.DateHistogramBucket)
	logger.Debug().Msgf("Querying job from elastic took %vms. Num results in aggregation=%v", res.Took, len(aggregateBuckets))
	if len(aggregateBuckets) > 0 {
		logger.Debug().Msgf("First bucket result: %v", aggregateBuckets[0])
	}

	var ret FilesystemStats
	for _, bucket := range aggregateBuckets {
		ret.Time = append(ret.Time, time.Unix(bucket.Key/1000, 0))
		ret.MetadataOPS = append(ret.MetadataOPS, f64(bucket.Aggregations["metadataops"].(*types.SumAggregate).Value, math.NaN()))
		ret.ReadBytes   = append(ret.ReadBytes,   f64(bucket.Aggregations["read_bytes"].(*types.SumAggregate).Value, math.NaN()))
		ret.ReadIOPS    = append(ret.ReadIOPS,    f64(bucket.Aggregations["read_iops"].(*types.SumAggregate).Value, math.NaN()))
		ret.WriteBytes  = append(ret.WriteBytes,  f64(bucket.Aggregations["write_bytes"].(*types.SumAggregate).Value, math.NaN()))
		ret.WriteIOPS   = append(ret.WriteIOPS,   f64(bucket.Aggregations["write_iops"].(*types.SumAggregate).Value, math.NaN()))

		LoadBuckets := bucket.Aggregations["load_one"].(*types.RangeAggregate).Buckets.([]types.RangeBucket)
		ret.Load = append(ret.Load, [5]int64{LoadBuckets[0].DocCount, LoadBuckets[1].DocCount, LoadBuckets[2].DocCount, LoadBuckets[3].DocCount, LoadBuckets[4].DocCount})
	}

	return &ret, nil
}

// helper functions
// return pointer to input arg
func ptr[T any](in T) *T {
	return &in
}

// return a float64 or default value if nil
func f64(in *types.Float64, def float64) float64 {
	if in == nil {
		logging.Error(fmt.Errorf("Calling f64 with a nil argument"), "Found a value returned from elastic to be a NULL value")
		return def
	}
	return float64(*in)
}
