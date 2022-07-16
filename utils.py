from exceptions import RequestError
import os
import re


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def read(filename):
    """Чтение данных из файла"""
    with open(f'{DATA_DIR}/{filename}', 'r') as file:
        while True:
            try:
                yield next(file).strip()
            except StopIteration:
                break


def get_query(data, cmd, value):
    """Составление запроса"""
    if cmd == 'filter':
        return filter_(data, value)
    elif cmd == 'map':
        return map_(data, value)
    elif cmd == 'unique':
        return unique(data)
    elif cmd == 'sort':
        return sort(data, value)
    elif cmd == 'limit':
        return limit(data, value)
    elif cmd == 'regex':
        return regex(data, value)
    else:
        raise RequestError('Проверьте правильность введенного запроса')


def filter_(data, value):
    """Функция фильтрации данных по заданному значению"""
    return filter(lambda row: value in row, data)


def map_(data, value):
    """Функция разделения исходных данных на колонки"""
    try:
        i = int(value)
        return (line.split()[i] for line in data if i < len(line.split()))
    except ValueError:
        raise RequestError('Проверьте правильность введенного аргумента')


def unique(data):
    """Функция, возвращающая только уникальные значения"""
    return set([row for row in data])


def sort(data, value):
    """Функция сортировки данных в указанном порядке"""
    if value not in ('asc', 'desc'):
        raise RequestError('Проверьте правильность введенного аргумента')
    return sorted([i for i in data], reverse=False if value == 'asc' else True)


def limit(data, value):
    """Функция, возвращающая заданное количество записей"""
    gen = iter(data)
    for i in range(int(value)):
        try:
            yield next(gen)
        except StopIteration:
            break


def regex(data, value):
    """Функция обработки регулярных выражений"""
    value = value.replace(' ', '+')
    reg = re.compile(value)
    gen = iter(data)
    while True:
        try:
            line = next(gen)
            for _ in re.findall(reg, line):
                yield line
        except StopIteration:
            break
