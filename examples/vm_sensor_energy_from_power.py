import datetime
import os
import yaml
import numpy as np
from scipy.integrate import cumulative_trapezoid

import matplotlib.pyplot as plt
from itertools import cycle
import requests

from common import Config, generate_token


with open(os.path.join(os.path.dirname(__file__), 'config.yaml')) as f:
    config: Config = yaml.safe_load(f)
    jobid = config['jobid']
    cluster = config['cluster']
    token = generate_token(config)
    auth_header = {'Authorization': f'Bearer {token}'}

    all_sensor_metrics = {
        'node_power': 'W',
        'node_energy': 'J',
        'gpu_power': 'W',
        'gpu_module_power': 'W',
        'cpu_power': 'W',
        'cpu_subsystem_power': 'W',
        'cpu_subsystem_energy': 'J',
        'aux_power': 'W'}

    # extract node_power
    metric = 'node_power'
    r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/energypower/{metric}', headers=auth_header)
    r.raise_for_status()
    response = r.json()
    time_power = np.array([datetime.datetime.fromtimestamp(x) for x in response['time']])
    nodes = response['nodes']
    nodeid1 = list(nodes.keys())[0]
    power = np.array(nodes[nodeid1]['series'][0]['data'])

    # extract node_energy
    metric = 'node_energy'
    r = requests.get(f'{config['base_url']}/metrics/{cluster}/{jobid}/energypower/{metric}', headers=auth_header)
    r.raise_for_status()
    response = r.json()
    time_energy = np.array([datetime.datetime.fromtimestamp(x) for x in response['time']])
    nodes = response['nodes']
    energy = np.array(nodes[nodeid1]['series'][0]['data'])

    # analysis - integrate power over time
    t_power_s = np.array([t.timestamp() for t in time_power])
    t_energy_s = np.array([t.timestamp() for t in time_energy])

    # cumulative integration of the power curve
    energy_from_power = cumulative_trapezoid(power, t_power_s - t_power_s[0], initial=0.0)
    # interpolate the sensor energy on the power time grid
    energy_on_power_grid = np.interp(t_power_s, t_energy_s, energy)

    if energy_on_power_grid[-1] > 0:
        with np.errstate(divide='ignore', invalid='ignore'):
            rel_diff = 100.0 * (energy_from_power - energy_on_power_grid) / energy_on_power_grid
        rel_diff[energy_on_power_grid < 0.01 * energy_on_power_grid[-1]] = np.nan
        total_rel_diff = 100.0 * (energy_from_power[-1] - energy_on_power_grid[-1]) / energy_on_power_grid[-1]
        print(f'final energy from power sensor : {energy_from_power[-1]:.6e} J')
        print(f'final energy from energy sensor: {energy_on_power_grid[-1]:.6e} J')
        print(f'difference                     : {total_rel_diff:+.2f}% of total sensor energy')
    else:
        rel_diff = None

    fig, (ax, ax_diff) = plt.subplots(2, 1, sharex=True, height_ratios=[3, 1])
    fig.set_figwidth(19)
    fig.set_figheight(10)
    ax.plot(time_energy, energy,
            marker='.',
            color='r',
            label='sensor energy')
    ax.plot(time_power, energy_from_power,
            marker='.',
            color='g',
            label='integrated power')
    ax.set_title('VictoriaMetrics sensor energy comparison')
    ax.set_ylabel('Energy (J)')
    ax.grid(True)
    ax.legend()

    if rel_diff is not None:
        ax_diff.plot(time_power, rel_diff, color='b', label='(integrated - sensor) / sensor')
        ax_diff.axhline(0, color='k', linewidth=0.5)
        ax_diff.axhline(0, color='k', linewidth=0.5)
        ax_diff.set_ylabel('Rel. diff (%)')
        ax_diff.grid(True)
        ax_diff.legend()
    ax_diff.set_xlabel('Time')
    fig.tight_layout()
    plt.show()
