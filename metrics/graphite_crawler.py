import requests
#import graphite_api

class GraphiteCrawler:
    pass

if __name__ == '__main__':
    #crawler = GraphiteCrawler()
    #req = 'http://155.230.118.227/metrics?' + 'target(storm.worker.t4-1-1648537497.ctenhank.)'
    graphite_url = 'http://155.230.118.227'
    req = graphite_url + '/metrics/index.json'
    res = requests.get(req, auth=('root', 'root'))
    
    indices = res.json()
    req = graphite_url + f'/metrics/find?query=storm.*'
    res = requests.get(req, auth=('root', 'root'))
    print(res.json())
    
    storm_indices = []
    for index in indices:
        if 'storm' in index:
            storm_indices.append(index)
    
    print(len(storm_indices))
    
    topology_indices = []
    worker_indices = []
    for index in storm_indices:
        if 'topology' in index:
            topology_indices.append(index)
        else:
            worker_indices.append(index)
            print(index)
            
    print(len(topology_indices), len(worker_indices))
    l1,  l2, l3, l4, l5, l6 = [], [], [], [], [], []
    for index in worker_indices:
        if 'ctenhank.__acker' in index:
            l1.append(index)
        elif 'ctenhank.__metrics' in index:
            l2.append(index)
        elif 'ctenhank.__system' in index:
            l3.append(index)
        elif 'ctenhank.count' in index:
            l4.append(index)
        elif 'ctenhank.split' in index:
            l5.append(index)
        elif 'ctenhank.spout' in index:
            l6.append(index)
    print(len(l1), len(l2), len(l3), len(l4), len(l5), len(l6))
            
            
        
    #print(storm_indices)
    #req = graphite_url + f"/render/?target=summarize({storm_index[0]},'1hour','last')&from=-1h&format=json"
    lat = ['storm.worker.wc_50_100_1_1_1_1-1-1648630270.3f7bfd634442.count.default.3.6700-__execute-latency-split:default',
        'storm.worker.wc_50_100_1_1_1_1-1-1648630270.3f7bfd634442.count.default.3.6700-__process-latency-split:default',
        'storm.worker.wc_50_100_1_1_1_1-1-1648630270.3f7bfd634442.split.default.4.6700-__execute-latency-spout:default',
        'storm.worker.wc_50_100_1_1_1_1-1-1648630270.3f7bfd634442.split.default.4.6700-__process-latency-spout:default']
    #for index in storm_indices:
    #    if 'latency' in index:
            
    #        print(index)
    
    for l in lat:
        req = graphite_url + f"/render/?target={l}&format=json"
        res = requests.get(req, auth=('root', 'root'))  
        print(res.json()[0])
        print(len(res.json()[0]['datapoints']))
        
            #
    #req = graphite_url + f"/render/?target={storm_indices[0]}&format=json"
    
    #req = graphite_url + f"/render/?target=storm.worker.wc_50_100_1_1_1_1-1-1648630270.3f7bfd634442.count.default.3.6700-__execute-latency-split:default&format=json"
    #res = requests.get(req, auth=('root', 'root'))
    #print(res.json()[0])
    #print(len(res.json()[0]['datapoints']))
    #print(res.json()[0])
            #indices.remove()
    #print(index)
    #if 'storm' in index:
    #    req = graphite_url + f'/metrics/find?query=storm.*'
    #    res = requests.get(req, auth=('root', 'root'))
    #    print(res.json())
    #    print(index)
            
        #print(index)
    
    #print(graphite_api.)
    