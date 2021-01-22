import sys
import subprocess
import json
import time

import generators.generators as gen
import generators.cities as city_gen


def add_ride(server_file, ride):
    ride = json.dumps(ride)
    out = subprocess.check_output(['client/add_ride.sh', server_file, ride])
    return json.loads(out)


def do_add_ride(server_file, ride):
    result = add_ride(server_file, ride)
    if result['result'] == 'success':
        arrow = ride['source'] + ' -> ' + ride['destination']
        provider = f'{ride["firstname"]} {ride["lastname"]} ({ride["phonenumber"]})'
        date = f"{ride['day']}/{ride['month']}/{ride['year']}"
        print(f'New ride {arrow} by {provider} on {date}, ride id: {result["ride-id"]}')
    else:
        print('New ride addition failed')


def main(cities_file, server_file):
    cities = city_gen.get_cities(cities_file)
    while True:
        ride = gen.ride(cities)
        do_add_ride(server_file, ride)
        time.sleep(0.1)


if __name__ == '__main__':
    main(cities_file=sys.argv[1],
         server_file=sys.argv[2])
