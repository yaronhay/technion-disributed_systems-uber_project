import sys
import random
from generators.places import randomPlaceName

MAX_ABS_X = 250
MAX_ABS_Y = 250


def random_point(mx, my):
    x = random.randint(-mx, mx)
    y = random.randint(-my, my)
    return x, y


def main(shard_count, city_count, file, shards_dir):
    had = set()

    def getName():
        nonlocal had
        name = randomPlaceName()
        while name in had:
            name = randomPlaceName()
        had.add(name)
        return name

    for i in range(1, shard_count + 1):
        with open(f'{shards_dir}/{i}.txt', 'w') as shard:
            for _ in range(city_count):
                x, y = random_point(MAX_ABS_X, MAX_ABS_Y)
                name = getName()
                shard.write(f'{name} {x} {y}\n')
                file.write(f'{name}\n')




def get_cities(path):
    with open(path) as file:
        return file.read().splitlines()