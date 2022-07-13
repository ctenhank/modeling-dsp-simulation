import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import seaborn as sns
import pickle as pkl

def get_pivot_from_insight(insight):
    ret = []
    for key in insight:
        print(f'{key}: {insight[key]}')
        pivot = float(insight[key].split(',')[2].split(':')[1])
        ret.append(pivot)
    return ret
    
def filter_array(arr, k):
    return [e for e in arr if e < k]

path = Path('../data/storm/wordcount/2')
files = [f for f in path.rglob('*') if f.is_file()]

print(f'files #: {len(files)}')

with open('../data/insight.pkl', 'rb') as f:
    insight = pkl.load(f)

parents = {}
for file in files:
    res = int(file.parent.stem.split('_')[1])
    
    if  str(file.parent) not in parents:
        parents[str(file.parent)] = set([str(file)])
    else:
        parents[str(file.parent)].add(str(file))

# Refine data
lat_insight = {}
unit = 1000000
for key in parents:
    print(key)
    res = Path(key).stem.split('_')[1]
    
    outimg_dir = Path('../img') / Path(key).stem
    outimg_dir.mkdir(exist_ok=True, parents=True)
    
    modified_key = str(Path(key).parent.parent / Path(key).stem)
    lat_insight[modified_key] = {}
    #print(Path(key).parent.parent / Path(key).stem)
    
    for file in parents[key]:
        print(f'  {file}')
        temp_dict = {}
        file = Path(file)
        #if not file.stem.__contains__('spout'):
        #    continue
        
        
        for comp in insight[modified_key]:
            if file.stem.__contains__(comp):
                component = comp
                break
        
        print(component)
        #print(component)
        #print(insight[key][component])
        pivots = get_pivot_from_insight(insight[modified_key][component]['processing']['point_details'])
        pivot = 0
        for p in pivots:
            if p > 50:
                pivot = p
                break
            
        print(pivots)
        plt.clf()
        with file.open('r') as f:
         
            pdata = []
            idata = []
            lines = f.readlines()
            for line in lines:
                if 'P:' in line:
                    d = line[3:]
                    if d != '':
                        pdata.append(int(line[3:]) / unit)
                elif 'I: ' in line:
                    d = line[3:]
                    if d != '':
                        idata.append(int(line[3:]) / unit)
            
            pdata = filter_array(pdata, np.percentile(pdata, pivot))
            pdata = np.array(pdata)
            
            #plt.tight_layout()
            #plt.ticklabel_format(style='plain')
            #plt.ticklabel_format(useOffset=False)
            #plt.autoscale(False)
            plt.clf()
            plt.plot(pdata)
            plt.savefig(f'{outimg_dir / file.stem}_line.png', dpi=80)
            #plt.show()
            #plt.tight_layout()
            #plt.ticklabel_format(style='plain')
            #plt.ticklabel_format(useOffset=False)
            #plt.ticklabel_format(axis='y', style='sci', scilimits=(0,0 ))
            #plt.autoscale(False)
            plt.clf()
            plt.hist(pdata)
            plt.savefig(f'{outimg_dir / file.stem}_hist.png', dpi=80)
            plt.clf()
            #plt.show()
            print(pdata.mean(), pdata.std())
            lat_insight[modified_key][component] ={
                "mean": pdata.mean(),
                "std": pdata.std() 
            }

import pickle as pkl
with open('../data/summary_insight.pkl', 'wb') as f:
    pkl.dump(lat_insight, f)
        
        