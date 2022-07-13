import socket
import time
import json
import pickle as pkl
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--outfile', default='interprocess22.pkl', type=str)
args = parser.parse_args()

HOST = "0.0.0.0"  # Standard loopback interface address (localhost)
PORT = 8082  # Port to listen on (non-privileged ports are > 1023)

q = []
cnt = 0
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    while cnt < 100000:
        conn, addr = s.accept()
        with conn:
            #print(f"Connected by {addr}")
            t = time.time_ns()
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                
                d = json.loads(data.decode('utf-8'))
                #print(d)
                time_diff = t-d['event_time']
                #print(time_diff)
                q.append(time_diff)
                #print(json.loads(data.decode('utf-8')))
                #print(data.split('-')[0])
                #print(str(data))
                #print(t - struct.unpack('f', struct.pack('f', str(data).split('-')[0])))
                #conn.sendall(data)
        #print(cnt)
        cnt += 1

with open(args.outfile, 'wb') as f:
    pkl.dump(q, f)