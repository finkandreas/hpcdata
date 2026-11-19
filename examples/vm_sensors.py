import datetime
import os
import yaml
import argparse

import matplotlib.pyplot as plt
from itertools import cycle
import requests

from common import Config, generate_token

def main(metrics_to_plot: list[str] | None = None) -> None:

    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        jobid = config['jobid']
        cluster = config['cluster']
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}

        all_sensor_metrics = {'node_power': 'W',
                              'node_energy': 'J',
                              'gpu_power': 'W',
                              'gpu_module_power': 'W',
                              'cpu_power': 'W',
                              'cpu_subsystem_power': 'W',
                              'cpu_subsystem_energy': 'J',
                              'aux_power': 'W'}

        # filter to requested metrics, if any were provided
        if metrics_to_plot:
            missing = [m for m in metrics_to_plot if m not in all_sensor_metrics]
            if missing:
                available = '\n  '.join(sorted(all_sensor_metrics.keys()))
                raise SystemExit(
                    f"Unknown metric(s): {', '.join(missing)}\n"
                    f"Available metrics:\n  {available}"
                )
            metrics = {metric: all_sensor_metrics[metric] for metric in metrics_to_plot}
        else:
            metrics = all_sensor_metrics

        for metric in metrics.keys():

            print('='*30)
            print(f'Fetching: {metric} ...')
            
            r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/energypower/{metric}', headers=auth_header)
            r.raise_for_status()

            response = r.json()
            
            time = [datetime.datetime.fromtimestamp(x) for x in response['time']]
            nodes = response['nodes']

            fig, ax = plt.subplots()
            fig.set_figwidth(19)
            fig.set_figheight(10)

            markers = ['.', '>', '^', 'v', 's']
            colors = cycle(plt.rcParams['axes.prop_cycle'].by_key()['color'])

            for nodeid, nodeData in nodes.items():
                color = next(colors)
                unit = nodeData['unit']
                series = nodeData['series']
                for i in range(len(series)):
                    if len(series)>1:
                        label = f'{nodeid} - {i}'
                    else:
                        label = f'{nodeid}'
                    ax.plot(time, series[i]['data'], 
                            marker=markers[i],
                            color=color,
                            label=label)

            ax.set_title(f'VictoriaMetrics sensor {metric}')
            ax.set_xlabel('Time')
            ax.set_ylabel(f'{metric} (' + metrics[metric] + ')')
            ax.grid(True)
            fig.legend()
            fig.tight_layout()
            plt.show()
            
            print(f'plot generated for {metric}.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plot sensor metrics from VictoriaMetrics.')
    parser.add_argument(
        '--metrics',
        nargs='*',
        help='Optional space-separated list of sensor metrics to plot. If omitted, all metrics are plotted.'
    )
    args = parser.parse_args()
    main(args.metrics)