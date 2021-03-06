from pathlib import Path
from metrics.submitter import DockerStormSubmitter

import json
import logging
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--network-metric', action='store_true')
    parser.add_argument('--storm-latency-metric', action='store_true')
    parser.add_argument('--config-file', type=str, default='./conf/config.json', help='Default is `./config/json`')
    parser.add_argument('--compose-path', type=str, default='./docker-compose-graphite.yml')
    args = parser.parse_args()
    
    try:
        with open(args.config_file, 'r') as file:
            exec_conf = json.load(file)
    except Exception:
        with open(args.config_file, 'w') as file:
            exec_conf = json.dump({
                "simulation": "wordcount",
                "num-workers": 1,
                "wc-num-source": 1,
                "wc-num-split": 1,
                "wc-num-count": 1,
                "wc-data-size": 100
            }, file)
        print('Created the default config.')
    
    
    if args.network_metric:
        pass
    
    if args.storm_latency_metric:
        submitter = DockerStormSubmitter()
        submitter.run(exec_conf,
                    #  cpu_capacities=[0.5, 1.0, 1.5],
                      cpu_capacities=[0.5, 1.0, 1.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.80, 0.85, 0.9, 0.95, 1.05, 1.10, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4, 1.45],
                      compose_path=args.compose_path,
                    #  data_rate=[1000, 200, 100, 50, 25, 15, 10, 7.5, 6.5, 5, 4, 3, 2, 1.75, 1.5, 1.4, 1.3 , 1.2, 1.1],
                    data_rate=[1],
                      finish=False)
        