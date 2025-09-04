import datetime
import os
import yaml

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests

from common import Config, generate_token, make_format

# change this lines for job/cluster
cluster = 'daint'
jobid = 1611324

if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}
        r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/capstor/global', headers=auth_header)
        r.raise_for_status()

        time = [datetime.datetime.fromtimestamp(x) for x in r.json()['time']]
        read_bw = r.json()['read_bandwidth']
        read_iops = r.json()['read_iops']
        write_bw = r.json()['write_bandwidth']
        write_iops = r.json()['write_iops']
        metadata_ops = r.json()['metadata_ops']
        # nodes_loadavg is an array where each entry is a 5-tuple with loadavg
        # ([0,20), [20,40), [40,80), [80,160), [160, inf))
        nodes_loadavg = r.json()['nodes_loadavg']

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)
        ax2 = ax.twinx()

        ax.plot(time, read_bw, label='Read bandwidth')
        ax.plot(time, write_bw, label='Write bandwidth')
        ax2.plot(time, read_iops, linestyle='dotted', label='Read IOPS')
        ax2.plot(time, write_iops, linestyle='dotted', label='Write IOPS')
        ax2.plot(time, metadata_ops, linestyle='dotted', label='Metadata OPS')

        ax.grid(True)
        ax2.fmt_xdata = mdates.DateFormatter('%H:%M')
        ax2.format_coord = make_format(ax2, ax)
        fig.legend()
        fig.tight_layout()

        fig, ax = plt.subplots()
        fig.set_figwidth(19)
        fig.set_figheight(10)
        ax.plot(time, [x[3] for x in nodes_loadavg], label='#nodes high load')
        ax.plot(time, [x[4] for x in nodes_loadavg], label='#nodes very high load')
        fig.legend()

        plt.tight_layout()
        plt.show()

