import datetime
import os
import yaml

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests

from common import Config, generate_token, make_format

if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        jobid = config['jobid']
        cluster = config['cluster']
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}
        r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/node/cpu', headers=auth_header)
        r.raise_for_status()

        time = [datetime.datetime.fromtimestamp(x) for x in r.json()['time']]
        nodeCpu = r.json()['nodes']

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)

        for nodeid, cpuData in nodeCpu.items():
            ax.plot(time, cpuData['user'], label=f'{nodeid} user CPU')
            ax.plot(time, cpuData['system'], label=f'{nodeid} system CPU')

        ax.grid(True)
        fig.legend()
        fig.tight_layout()
        plt.show()


