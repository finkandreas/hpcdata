import datetime
import os
import re
import yaml
import argparse

import matplotlib.pyplot as plt
import requests

from common import Config, generate_token, interpolate


def parse_dcgm_metrics(go_file: str) -> dict[str, str]:
    """Parse the dcgmMetrics map from the Go source file."""
    metrics: dict[str, str] = {}
    with open(go_file) as f:
        content = f.read()

    match = re.search(r'var\s+dcgmMetrics\s+=\s+map\[string\]string\{([^}]*)\}', content, re.DOTALL)
    if not match:
        raise RuntimeError('dcgmMetrics map not found in ' + go_file)

    for key, value in re.findall(r'"([^"]+)"\s*:\s*"([^"]*)"', match.group(1)):
        metrics[key] = '' if value in ['count', 'gauge'] else f' ({value})'

    return metrics


def main(metrics_to_plot: list[str] | None = None, max_hole_size: int = -1) -> None:

    if max_hole_size < -1:
        raise ValueError(f'Invalid max_hole_size = {max_hole_size}. Should be >=-1.')

    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        jobid = config['jobid']
        cluster = config['cluster']
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}

        # pipe all DCGM metrics from hpcdata/victoriametrics/metrics.go into a dict
        all_dcgm_metrics = parse_dcgm_metrics(
            os.path.join(os.path.dirname(__file__), '..', 'victoriametrics', 'metrics.go')
        )

        # filter to requested metrics, if any were provided
        if metrics_to_plot:
            missing = [m for m in metrics_to_plot if m not in all_dcgm_metrics]
            if missing:
                available = '\n  '.join(sorted(all_dcgm_metrics.keys()))
                raise SystemExit(
                    f"Unknown metric(s): {', '.join(missing)}\n"
                    f"Available metrics:\n  {available}"
                )
            metrics = {metric: all_dcgm_metrics[metric] for metric in metrics_to_plot}

        for metric in metrics.keys():

            print('='*30)
            print(f'Fetching: {metric} ...')
            
            r = requests.get(f"{config['base_url']}/metrics/{cluster}/{jobid}/vm/gpu/{metric}", headers=auth_header)
            r.raise_for_status()

            response = r.json()
            time = [datetime.datetime.fromtimestamp(x) for x in response['time']]
            nodes = response['nodes']

            fig, ax = plt.subplots()
            fig.set_figwidth(19)
            fig.set_figheight(10)

            markers = ['>', '^', 'v', 's']

            for nodeid, nodeData in nodes.items():
                for gpuData in nodeData[metric]:
                    gpu_id = int(gpuData["gpu_id"])
                    if max_hole_size == -1:
                        data = gpuData['data']
                    else:
                        data = interpolate(
                            y=gpuData['data'],
                            x=time,
                            max_hole_size=max_hole_size)
                    ax.plot(time, data,
                            marker=markers[gpu_id % len(markers)],
                            fillstyle='none',
                            markersize=8,
                            label=f'GPU {gpu_id}')
                # only plot data of the first node
                break

            ax.set_title(f'VictoriaMetrics DCGM {metric}')
            ax.set_xlabel('Time')
            ax.set_ylabel(f'{metric}' + metrics[metric])
            ax.grid(True)
            fig.legend()
            fig.tight_layout()
            plt.show()

            print(f'plot generated for {metric}.')
            #input("Press Enter to continue...")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plot DCGM metrics from VictoriaMetrics.')
    parser.add_argument(
        '--max_hole_size',
        type=int,
        default=-1,
        help='Optional maximum hole size to interpolate. If omitted, holes are not interpolated. If 0, all holes are interpolated.'
    )
    parser.add_argument(
        '--metrics',
        nargs='*',
        help='Optional space-separated list of DCGM metrics to plot. If omitted, all metrics are plotted.'
    )
    args = parser.parse_args()
    if args.max_hole_size < -1:
        parser.error('--max_hole_size must be >= -1')
    main(args.metrics, args.max_hole_size)
