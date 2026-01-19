import datetime
import os
import yaml

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests

from common import Config, generate_token, make_format

# change this lines for job/cluster
cluster = 'daint'
jobid = 2533837

if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}
        r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/custom?context=cpu:all&name=utilization', headers=auth_header)
        r.raise_for_status()

        data = r.json()
        all_nodes_in_job = data.keys()

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)

        for node_id in all_nodes_in_job:
            for custom_data in data[node_id]:
                ax.plot(custom_data['time'], [float(x) for x in custom_data['value']], label=f'{custom_data["name"]} - {custom_data["context"]}')
                # only plot first custom_data and first node
                break

        ax.grid(True)
        fig.legend()
        fig.tight_layout()
        plt.show()

