from collections import Counter
import socket
import time

import ray

ray.init(address='auto', _redis_password='5241590000000000')

# import time

# @ray.remote
# def f():
#     time.sleep(0.01)
#     return ray._private.services.get_node_ip_address()

# # Get a list of the IP addresses of the nodes that have joined the cluster.
# print(set(ray.get([f.remote() for _ in range(1000)])))


print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))

@ray.remote
def f():
    time.sleep(0.001)
    # Return IP address.
    return socket.gethostbyname(socket.gethostname())

object_ids = [f.remote() for _ in range(10000)]
ip_addresses = ray.get(object_ids)

print('Tasks executed')
for ip_address, num_tasks in Counter(ip_addresses).items():
    print('    {} tasks on {}'.format(num_tasks, ip_address))