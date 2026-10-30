import datetime
import os
import yaml

import matplotlib.pyplot as plt
import requests

from common import Config, generate_token


def fetch(url: str, headers: dict):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


if __name__ == '__main__':
    with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
        config: Config = yaml.safe_load(f)
        jobid = config['jobid']
        cluster = config['cluster']
        metric = 'gpu_temp'
        token = generate_token(config)
        auth_header = {'Authorization': f'Bearer {token}'}

        es_url = f"{config['base_url']}/metrics/{cluster}/{jobid}/gpu/temperature"
        vm_url = f"{config['base_url']}/metrics/{cluster}/{jobid}/gpu/{metric}"

        es_data = fetch(es_url, auth_header)
        vm_data = fetch(vm_url, auth_header)

        es_time = [datetime.datetime.fromtimestamp(x) for x in es_data['time']]
        vm_time = [datetime.datetime.fromtimestamp(x) for x in vm_data['time']]

        all_nodes = set(es_data['nodes'].keys()) | set(vm_data['nodes'].keys())

        cols = ['r', 'b', 'g', 'm']

        for nodeid in sorted(all_nodes):
            # ES returns a list of objects per GPU
            es_gpus = {g['gpu_id']: g for g in es_data['nodes'].get(nodeid, [])}
            # VM returns a dict with a metric key containing a list of objects per GPU
            vm_node = vm_data['nodes'].get(nodeid, {})
            vm_gpus = {g['gpu_id']: g for g in vm_node.get(metric, [])}

            fig, ax = plt.subplots()
            fig.set_figwidth(19)
            fig.set_figheight(10)

            all_gpu_ids = set(es_gpus.keys()) | set(vm_gpus.keys())
            for gpu_id in sorted(all_gpu_ids):
                if gpu_id in es_gpus:
                    g = es_gpus[gpu_id]
                    ax.plot(es_time, g['temperature'],
                            color=cols[gpu_id],
                            label=f'ES GPU {gpu_id}')
                if gpu_id in vm_gpus:
                    g = vm_gpus[gpu_id]
                    ax.plot(vm_time, g['data'],
                            color=cols[gpu_id],
                            linestyle='--',
                            label=f'VM GPU {gpu_id}')

                if gpu_id in es_gpus and gpu_id in vm_gpus:
                    es_mean = sum(es_gpus[gpu_id]['temperature']) / len(es_gpus[gpu_id]['temperature'])
                    vm_mean = sum(vm_gpus[gpu_id]['data']) / len(vm_gpus[gpu_id]['data'])
                    print(
                        f'{nodeid} GPU {gpu_id}: '
                        f'ES points={len(es_gpus[gpu_id]["temperature"])}, '
                        f'VM points={len(vm_gpus[gpu_id]["data"])}, '
                        f'ES mean={es_mean:.2f}°C, '
                        f'VM mean={vm_mean:.2f}°C'
                    )
            
            ax.set_title(f'GPU temperature comparison for {nodeid}')
            ax.set_xlabel('Time')
            ax.set_ylabel('Temperature (°C)')
            ax.grid(True)
            fig.legend()
            fig.tight_layout()
            plt.show()
