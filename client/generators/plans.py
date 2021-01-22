import cities
import generators as gens
import json
import sys


def main(path):
    clist = cities.get_cities(path)
    ride = gens.path(clist)
    s = json.dumps(ride, indent=2)
    print(s)


if __name__ == '__main__':
    main(sys.argv[1])
