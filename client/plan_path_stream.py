import sys
import subprocess
import json
import time

import generators.generators as gen
import generators.cities as city_gen


def plan_path(server_file, ride):
    ride = json.dumps(ride)
    out = subprocess.check_output(['client/plan_path.sh', server_file, ride])
    return json.loads(out)


def do_plan_path(server_file, plan):
    result = plan_path(server_file, plan)
    transaction_id = result['system-transaction-id']
    arrow = ' -> '.join(plan['cities'])
    consumer = f'{plan["firstname"]} {plan["lastname"]} ({plan["phonenumber"]})'
    date = f"{plan['day']}/{plan['month']}/{plan['year']}"
    desc = f'Path Planning on {date} for {consumer} : {arrow}'

    if result['result'] == 'success':
        rides = result['rides']['rides']
        ride_list = []

        for i, ride in enumerate(rides):
            provider = ride["provider"]
            provider = f'{provider["firstname"]} {provider["lastname"]} ({provider["phonenumber"]})'

            id = ride["id"]

            arrow = f"{ride['source']['name']} -> {ride['destination']['name']}"

            ride_list.append(f"\n\t{i + 1}) {arrow} with {provider} (Ride ID : {id})")

        print(f'{desc}\n\t Was successful (Transaction ID {transaction_id}):{"".join(ride_list)}')
    else:
        print(f'{desc}\n\t Failed (Transaction ID {transaction_id})')


def main(cities_file, server_file):
    cities = city_gen.get_cities(cities_file)
    while True:
        plan = gen.path(cities)
        do_plan_path(server_file, plan)
        time.sleep(0.75)


if __name__ == '__main__':
    main(cities_file=sys.argv[1],
         server_file=sys.argv[2])
