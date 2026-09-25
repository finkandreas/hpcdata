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
        r = requests.get(f'{config['base_url']}/logs/{cluster}/{jobid}/syslog', headers=auth_header)
        r.raise_for_status()

        for nid, syslog_data in r.json().items():
            print(f"Syslog messages for {nid=}")
            print(f"\t{'Time':^6}{'Priority':^8}{'Source':^16}Message")
            for timestamp, msg_data in zip(syslog_data["time"], syslog_data["syslog"]):
                time = datetime.datetime.fromtimestamp(timestamp).strftime("%H:%M")
                print(f'\t{time:^6}{msg_data["priority"]:^8}{msg_data["source"]:^16}{msg_data["message"]}')
