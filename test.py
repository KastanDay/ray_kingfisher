import ray
ray.init()

# A regular Python function.
def my_function():
    return 1

# By adding the `@ray.remote` decorator, a regular Python function
# becomes a Ray remote function.
@ray.remote
def my_function():
    return 1

# To invoke this remote function, use the `remote` method.
# This will immediately return an object ref (a future) and then create
# a task that will be executed on a worker process.
obj_ref = my_function.remote()

# The result can be retrieved with ``ray.get``.
assert ray.get(obj_ref) == 1

@ray.remote
def slow_function():
    time.sleep(10)
    return 1

# Invocations of Ray remote functions happen in parallel.
# All computation is performed in the background, driven by Ray's internal event loop.
for i in range(4):
    # This doesn't block.
    slow_function.remote()
    print(i)

@ray.remote
def function_with_an_argument(value):
    return value + 1


obj_ref1 = my_function.remote()
assert ray.get(obj_ref1) == 1

# You can pass an object ref as an argument to another Ray remote function.
obj_ref2 = function_with_an_argument.remote(obj_ref1)
assert ray.get(obj_ref2) == 2

# Get the value of one object ref.
obj_ref = ray.put(1)
assert ray.get(obj_ref) == 1

# Get the values of multiple object refs in parallel.
assert ray.get([ray.put(i) for i in range(3)]) == [0, 1, 2]

# You can also set a timeout to return early from a ``get`` that's blocking for too long.
from ray.exceptions import GetTimeoutError

@ray.remote
def long_running_function():
    time.sleep(8)

obj_ref = long_running_function.remote()
try:
    ray.get(obj_ref, timeout=4)
except GetTimeoutError:
    print("`get` timed out.")
