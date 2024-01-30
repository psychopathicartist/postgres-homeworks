"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
import psycopg2

user_password = os.getenv('POSTGRES_PASSWORD')


def make_list_from_csv(file_name: str) -> list:
    """
    Возвращает список из кортежей, полученных из файла csv
    """
    list_of_tuples = []
    with open(file_name, encoding='utf-8', newline='') as csvfile:
        data = csv.DictReader(csvfile)
        for data_dict in data:
            tuple_data = tuple(data_dict.values())
            list_of_tuples.append(tuple_data)
    return list_of_tuples


# Перевод csv файлов в список
customers_data = make_list_from_csv('north_data/customers_data.csv')
employees_data = make_list_from_csv('north_data/employees_data.csv')
orders_data = make_list_from_csv('north_data/orders_data.csv')

# Подключение к базе данных
with psycopg2.connect(host="localhost", database="north", user="postgres", password=f"{user_password}") as conn:
    with conn.cursor() as cur:

        # Добавление данных в базу данных
        cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", customers_data)
        cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_data)
        cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_data)

conn.close()
