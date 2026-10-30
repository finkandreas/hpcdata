package victoriametrics

const dcgmMetricPrefix = "cray_storage.dcgm."

var dcgmMetrics = map[string]string{
	"sm_clock":                          "MHz",
	"memory_clock":                      "MHz",
	"memory_temp":                       "°C",
	"gpu_temp":                          "°C",
	"power_usage":                       "W",
	"total_energy_consumption":          "mJ",
	"power_management_limit":            "W",
	"power_management_limit_min":        "W",
	"power_management_limit_max":        "W",
	"power_management_limit_default":    "W",
	"enforced_power_limit":              "W",
	"pcie_tx_throughput":                "KB/s",
	"pcie_rx_throughput":                "KB/s",
	"pcie_replay_counter":               "count",
	"gpu_utilization":                   "%",
	"mem_copy_utilization":              "%",
	"enc_utilization":                   "%",
	"dec_utilization":                   "%",
	"xid_errors":                        "gauge",
	"power_violation":                   "ns",
	"thermal_violation":                 "ns",
	"sync_boost_violation":              "ns",
	"board_limit_violation":             "ns",
	"low_util_violation":                "ns",
	"reliability_violation":             "ns",
	"fb_free":                           "MiB",
	"fb_used":                           "MiB",
	"fb_resv":                           "MiB",
	"ecc_sbe_volatile_total":            "count",
	"ecc_dbe_volatile_total":            "count",
	"ecc_sbe_aggregate_total":           "count",
	"ecc_dbe_aggregate_total":           "count",
	"retired_pages_sbe":                 "count",
	"retired_pages_dbe":                 "count",
	"retired_pages_pending":             "count", // depends on documentation version. might be a boolean gauge
	"uncorrectable_remapped_rows":       "count",
	"correctable_remapped_rows":         "count",
	"row_remap_failure":                 "gauge",
	"nvlink_flit_crc_error_count_total": "count",
	"nvlink_data_crc_error_count_total": "count",
	"nvlink_replay_error_count_total":   "count",
	"nvlink_recovery_error_count_total": "count",
	"nvlink_bandwidth_total":            "MB/s",
	"vgpu_instance_license_status":      "gauge",
	"sm_active":                         "%",
	"sm_occupancy":                      "%",
	"tensor_active":                     "%",
	"dram_active":                       "%",
	"fp64_active":                       "%",
	"fp32_active":                       "%",
	"fp16_active":                       "%",
	"pcie_tx_bytes":                     "B/s",
	"pcie_rx_bytes":                     "B/s",
	"nvlink_tx_bytes":                   "B/s",
	"nvlink_rx_bytes":                   "B/s",
}

type MetricInfo struct {
	VMMetric string // full Prometheus metric name as stored in VictoriaMetrics
	Unit     string // metric unit
}

// map a short DCGM metric name to its VM full metric name and unit
var SupportedMetrics = func() map[string]MetricInfo {
	m := make(map[string]MetricInfo, len(dcgmMetrics))
	for name, unit := range dcgmMetrics {
		m[name] = MetricInfo{VMMetric: dcgmMetricPrefix + name, Unit: unit}
	}
	return m
}()
