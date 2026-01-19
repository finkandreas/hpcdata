import datetime
import logging
import os
import requests
import time

def get_cpu_jiffies():
    with open('/proc/stat', 'r') as f:
        line = f.readline()
        while not line.startswith('cpu '):
            line = f.readline()
        parts = list(map(int, line.strip().split()[1:]))  # Skip "cpu"
        user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice = parts
        # Exclude guest/guest_nice (already in user/nice)
        total = user + nice + system + idle + iowait + irq + softirq + steal
        idle_total = idle + iowait  # Include iowait in idle
        return total, idle_total


if __name__ == '__main__':
    hostname = open('/etc/hostname').read().strip()
    xname = open('/etc/cray/xname').read().strip()
    metric_name = 'utilization'
    context = 'cpu:all'
    cluster = os.environ['CLUSTER_NAME']
    job_id = os.environ['SLURM_JOB_ID']

    data = {
        'name': metric_name,
        'context': context,
        'xname': xname,
        'timestamp': datetime.datetime.now().timestamp(),
        'value': '',
    }
    url = f'https://hpcdata.vserverli.de/metrics/{cluster}/{job_id}/{hostname}/custom'

    # Get initial jiffies
    t0_total, t0_idle = get_cpu_jiffies()
    while True:
        try:
            time.sleep(1)  # Wait 1 second
            t1_total, t1_idle = get_cpu_jiffies()

            # Calculate deltas
            delta_total = t1_total - t0_total
            delta_idle = t1_idle - t0_idle
            cpu_usage = (1 - delta_idle / delta_total) * 100  # (total - idle)/total *100

            # send data
            # value must be a string
            data['value'] = f'{cpu_usage}'
            data['timestamp'] = int(datetime.datetime.now().timestamp())
            r = requests.post(url, json=data, timeout=(3,3))

            t0_total, t0_idle = t1_total, t1_idle
        except Exception as e:
            logging.error(f'Caught an exception trying to push the data. Exception={e}')
