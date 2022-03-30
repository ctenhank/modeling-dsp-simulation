
import time
from typing import List
import docker
from pathlib import Path
import subprocess
from tqdm import tqdm
import signal
import logging

class DockerStormSubmitter:
    JOBS = ['wordcount']
    def __init__(self):        
        self._client = docker.from_env()
        self._executed_topology = None
        self._init_signal_handler()
        
        
    def _init_signal_handler(self):
        signal.signal(signal.SIGINT, self._shutdown)
        
        
    def _shutdown(self, signum, frame):
        print(f'Shutdown: {signum}, {frame}')
        #subprocess.run(['docker-compose', '-f', f'{Path.cwd() / + "docker-compose-graphite.yml"}', 'down'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        #self._remove_path(DockerStormSubmitter.TMP_PATH)      
        exit(signum)  
        
    
    def _remove_path(self, path):
        if type(path) is str:
            path = Path(path)
            
        if path.is_dir():
            paths: List[Path] = []
            for p in path.rglob('*'):
                paths.append(p)        
            
            for p in paths:
                if p.is_file():
                    p.unlink()
                    paths.remove(p)
            
            for p in paths:
                p.rmdir()
            path.rmdir()
        else:
            path.unlink()
            
    def _command_parser(self, args:list):
        if type(args) is not list:
            print('You should pass the type of argument `args` as `list` of `str`')
            exit(1)
        
        ret = ''
        for arg in args:
            ret += str(arg) + ' '
            
        return ret[:-1]
    

    def _wc_job(self, conf: dict, cap: float):
        jar_path = '/jars/wc.jar'
        topology_path = 'kr.ac.knu.sslab.storm.examples.topology.WordCountTopology'
        num_worker = conf['num-workers']
        num_source = conf['wc-num-source']
        num_split = conf['wc-num-split']
        num_count = conf['wc-num-count']
        data_size = conf['wc-data-size']
        
        topology_name = f'wc_{str(int(cap * 100))}_{data_size}_{num_worker}_{num_source}_{num_split}_{num_count}'
        self._executed_topology = topology_name
        args = ['storm', 'jar', jar_path, topology_path, topology_name, num_worker, num_source, num_split, num_count, data_size]
        return self._command_parser(args)
    
    
    def _kill_job(self, nimbus):
        cont = self._client.containers.get(nimbus)
            
        if self._executed_topology is not None:
            res = cont.exec_run(['storm', 'kill', self._executed_topology])
            if res[0] == 0:
                print(f'Succeed to kill the topology {self._executed_topology}')
                
    
    def _submit_job(self, conf: dict, cap: float, nimbus):
        """_summary_

        Args:
            exec_conf (_type_): _description_
        """
        
        cont = self._client.containers.get(nimbus)
        if conf['simulation'] in DockerStormSubmitter.JOBS:
            command = None
            if conf['simulation'] == 'wordcount':
                command = self._wc_job(conf, cap)
                
            if command is not None:
                res = cont.exec_run(command)
                print(res)
                if res[0] == 0:
                    print(f'Succeed to execute the topology {self._executed_topology}')
                    return True
                #else:
                
        return False
        
        
    def _update_supervisor_cpus(self, cap):
        supervisor_name = None
        for cont in self._client.containers.list():
            if Path.cwd().name + '_supervisor' in cont.name:
                supervisor_name = cont.name
        
        if supervisor_name is not None:
            subprocess.run(['docker', 'update', '--cpus', str(cap), supervisor_name], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            print(f'Updated the cpu limit of container {supervisor_name} to {cap}')
        else:
            print(f'No such supervisor: {supervisor_name}')
            exit(2)
            
    
    def _set_volume_path(self, conf):
        # Modified Storm config file(storm.yaml)
        conf['services']['nimbus']['volumes'][0] = str(Path.cwd() / 'conf' / 'storm' / 'storm.yaml') + ':/apache-storm-2.3.0/conf/storm.yaml' 
        conf['services']['supervisor']['volumes'][0] = str(Path.cwd() / 'conf' / 'storm' / 'storm.yaml') + ':/apache-storm-2.3.0/conf/storm.yaml' 
        conf['services']['ui']['volumes'][0] = str(Path.cwd() / 'conf' / 'storm' / 'storm.yaml') + ':/apache-storm-2.3.0/conf/storm.yaml' 
        return conf
    
    
    def _wait_for_job(self, conf):
        period = 630
        if 'period' in conf.keys():
            period = conf['collect_period']
        
        print(f'Started the job, the below is progress bar up to {period} seconds')
        for _ in tqdm(range(100), ncols=100, desc=f'Job Progress'):
            time.sleep(period / 100)
            
            
    def _get_nimbus_name(self, compose_path):
        cmd = ['docker-compose', '-f', compose_path, 'ps', 'nimbus']
        ps = subprocess.run(cmd, capture_output=True, check=True)
        result = ps.stdout.decode('utf8').split('\n')[2:]
        return [r.split(' ')[0] for r in result][:-1]
    
    def _is_running(self, compose_path):
        cmd = ['docker-compose', '-f', compose_path, 'ps']
        ps = subprocess.run(cmd, capture_output=True, check=True)
        result = ps.stdout.decode('utf8').split('\n')[2:-1]
        return len(result) == 0

    
    def run(self, 
            exec_conf,
            compose_path, 
            cpu_capacities,
            finish=True):
        """_summary_

        Args:
            exec_conf (_type_): _description_
            compose_path (str, optional): _description_. Defaults to './docker-compose.yml'.
            cpu_capacities (list, optional): _description_. Defaults to [0.5, 1.0, 1.5, 2.0, 3.0, 4.0].
            finish (bool, optional): Determine turning off the docker-compose at the end. The default is to True
        """
    
        
        #if not self._is_running(compose_path):
            
        #first_run = True
        print('=' * 100)
        if self._is_running(compose_path):
            print('Start the docker-compose...')
            subprocess.run(['docker-compose', '-f', compose_path, 'up', '-d'])
        else:
            print('These are already running services...')
        print('=' * 100)
        
        time.sleep(10)
        
        # We suppose there is only one nimbus in the cluster
        nimbus = self._get_nimbus_name(compose_path)[0]
        
        cnt = 0
        print('=' * 100)
        print(f'Experiments starts... : total exp # {len(cpu_capacities)}')
        print('-' * 100)
        for cap in cpu_capacities:
            print(f'Experiment {cnt}: CPU capability {cap}')
            
            # Get supervisor name in the docker-compose
            self._update_supervisor_cpus(cap)
            self._submit_job(exec_conf, cap, nimbus)
            self._wait_for_job(exec_conf) 
            self._kill_job(nimbus)
            cnt += 1
            print('-' * 100)
        print('=' * 100)
                
        if finish:
            print('Finished the docker-compose...')
            print('-' * 100)
            subprocess.run(['docker-compose', '-f', compose_path, 'down'])
            print('-' * 100)