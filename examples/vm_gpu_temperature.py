import datetime
import os
import yaml

import matplotlib.pyplot as plt
import requests

from common import Config, generate_token

if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        jobid = config['jobid']
        cluster = config['cluster']
        metric = 'gpu_temp'
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}
        r = requests.get(f"{config['base_url']}/metrics/{cluster}/{jobid}/gpu/{metric}", headers=auth_header)
        r.raise_for_status()

        response = r.json()
        time = [datetime.datetime.fromtimestamp(x) for x in response['time']]
        nodes = response['nodes']

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)

        for nodeid, nodeData in nodes.items():
            for gpuData in nodeData[metric]:
                ax.plot(time, gpuData['data'],
                        marker='.',
                        label=f'GPU {gpuData["gpu_id"]}')
            # only plot GPU temperatures of the first node
            break

        ax.set_title(f'VictoriaMetrics {metric}')
        ax.set_xlabel('Time')
        ax.set_ylabel('Temperature (°C)')
        ax.grid(True)
        fig.legend()
        fig.tight_layout()
        plt.show()
