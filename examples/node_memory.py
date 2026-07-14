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
        r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/node/memory', headers=auth_header)
        r.raise_for_status()

        time = [datetime.datetime.fromtimestamp(x) for x in r.json()['time']]
        nodeMemory = r.json()['nodes']

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)

        for nodeid, memoryData in nodeMemory.items():
            free = [ x/1024/1024 for x in memoryData['free'] ]
            cache = [ x/1024/1024 for x in memoryData['cache'] ]
            buffer = [ x/1024/1024 for x in memoryData['buffer'] ]
            ax.plot(time, free, label=f'{nodeid} free')
            ax.plot(time, cache, label=f'{nodeid} cache')
            ax.plot(time, buffer, label=f'{nodeid} buffer')

        ax.grid(True)
        fig.legend()
        fig.tight_layout()
        plt.show()


