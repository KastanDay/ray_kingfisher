from collections import Counter
import os
import socket
import sys
import time
import ray

# num_cpus = int(sys.argv[1])

ray.init(address=os.environ["ip_head"])

# print("Nodes in the Ray cluster:")
# print(ray.nodes())

print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))


@ray.remote
def f():
    time.sleep(0.01)
    return socket.gethostbyname(socket.gethostname())


# The following takes one second (assuming that
# ray was able to access all of the allocated nodes).
# for i in range(60):
#     start = time.time()
#     ip_addresses = ray.get([f.remote() for _ in range(num_cpus)])
#     print(Counter(ip_addresses))
#     end = time.time()
#     print(end - start)

object_ids = [f.remote() for _ in range(10000)]
ip_addresses = ray.get(object_ids)

print('Tasks executed')
for ip_address, num_tasks in Counter(ip_addresses).items():
    print('    {} tasks on {}'.format(num_tasks, ip_address))