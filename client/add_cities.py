import sys
from generators.cities import main

if __name__ == '__main__':
    with open(sys.argv[3], 'w') as file:
        main(shard_count=int(sys.argv[1]),
             city_count=int(sys.argv[2]),
             file=file, shards_dir=sys.argv[4])