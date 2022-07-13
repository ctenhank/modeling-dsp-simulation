from collections import deque
import threading
from threading import Thread
import time
import numpy as np
import matplotlib.pyplot as plt

import pickle as pkl



check = threading.Condition()

def func1(q: deque):
    #print ("funn1 started")
    stime = time.time_ns()
    check.acquire()
    check.wait()
    #print ("got permission")
    latency = time.time_ns() - stime
    #print(time.time() - stime)
    #print ("funn1 finished")
    q.append(latency)
    

def func2(q: deque):
    #print ("func2 started")
    stime = time.time_ns()
    check.acquire()
    #time.sleep(2)
    check.notify()
    check.release()
    #time.sleep(2)
    latency = time.time_ns() - stime
    #print(time.time() - stime)
    #print ("func2 finished")
    q.append(latency)

q = deque()
if __name__ == '__main__':
    t1=Thread(target = func1, args=(q,))
    t2=Thread(target = func2, args=(q,))
    #for _ in range(10000):
    #    #t1=Thread(target = func1, args=(q,))
    #    #t2=Thread(target = func2, args=(q,))
    #    t1=Thread(target = func1, args=(q,))
    #    t2=Thread(target = func2, args=(q,))    
    #    t1.start()
    #    t2.start()
    #    #t1.join()
    #    #t2.join()
    
    t1=Thread(target = func1, args=(q,))
    t2=Thread(target = func2, args=(q,))    
    t1.start()
    t2.start()
    
    arr = list(q)
    with open('interthread.pkl', 'wb') as f:
        pkl.dump(arr, f)
    #arr = np.array(list(q))
    #plt.plot(arr)
    #plt.show()
    
        
    #print(q.__len__())