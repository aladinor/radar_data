from fast_map import fast_map_async, fast_map
import time
from radar_utils import timer_func


def io_and_cpu_expensive_function(x):
    time.sleep(1)
    for i in range(10 ** 6):
        pass
    return x*x


def on_result(result):
    print(result)


def on_done():
    print('all done')


def task_with_multiple_params(a, b):
    return a + ' - ' + b


@timer_func
def main():
    print('Using function with multiple parameters:')
    for s in fast_map(task_with_multiple_params, ['apple', 'banana', 'cherry'], ['orange', 'lemon', 'pineapple']):
        print(s)

    # returns a thread
    t = fast_map_async(
        io_and_cpu_expensive_function,
        range(50),
        on_result=on_result,
        on_done=on_done,
        threads_limit=100
    )
    t.join()
    print()



if __name__ == "__main__":
    main()
