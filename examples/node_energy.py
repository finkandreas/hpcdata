import datetime
import os
import yaml

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests

from common import Config, generate_token, make_format

# change this lines for job/cluster
cluster = 'daint'
jobid = 2985215
jobid = 2997947

if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}
        r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/node/energy', headers=auth_header)
        r.raise_for_status()

        time = [datetime.datetime.fromtimestamp(x) for x in r.json()['time']]
        nodeEnergy = r.json()['nodes']

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)

        for nodeid, energyData in nodeEnergy.items():
            ax.plot(time, energyData['energy'], label=f'{nodeid}')

        ax.grid(True)
        fig.legend()
        fig.tight_layout()
        plt.show()

