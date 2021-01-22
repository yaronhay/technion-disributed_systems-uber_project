import names
import random


def phone_number():
    result = []

    def add():
        result.append(str(random.randint(0, 9)))

    for _ in range(3):
        add()
    result.append('-')
    for _ in range(3):
        add()
    result.append('-')
    for _ in range(4):
        add()
    return ''.join(result)


def person():
    return names.get_first_name(), names.get_last_name(), phone_number()


YEAR_RANGE = (2021, 2021)
MONTH_RANGE = (1, 1)
DAY_RANGE = (1, 15)


def date():
    y = random.randint(*YEAR_RANGE)
    m = random.randint(*MONTH_RANGE)
    d = random.randint(*DAY_RANGE)
    return d, m, y


MIN_VACANCIES = 3
MAX_VACANCIES = 15

MIN_DEV = 0.5
MAX_DEV = 300


def ride(cities):
    d, m, y = date()
    first, last, phone = person()
    vac = random.randint(MIN_VACANCIES, MAX_VACANCIES)
    dev = round(random.uniform(MIN_DEV, MAX_DEV), 2)
    src = random.choice(cities)
    dst = random.choice(cities)
    while src == dst:
        dst = random.choice(cities)
    ride = {
        "day": d,
        "month": m,
        "year": y,
        "source": src,
        "destination": dst,
        "firstname": first,
        "lastname": last,
        "phonenumber": phone,
        "permitted-deviation": dev,
        "vacancies": vac
    }
    return ride


def city_path(length, cities):
    length -= 1
    path = [random.choice(cities)]
    last = path[0]

    for _ in range(length):
        city = random.choice(cities)
        while city == last:
            city = random.choice(cities)
        path.append(city)
        last = city

    return path


MIN_PATH_LEN = 2
MAX_PATH_LEN = 4


def path(cities):
    d, m, y = date()
    first, last, phone = person()
    length = random.randint(MIN_PATH_LEN, MAX_PATH_LEN)
    city_p = city_path(length, cities)

    path = {
        "day": d,
        "month": m,
        "year": y,
        "firstname": first,
        "lastname": last,
        "phonenumber": phone,
        "cities": city_p
    }
    return path
