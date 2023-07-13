"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv


# соединение с БД
conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='12345'
)

try:
    with conn:
        # создание курсора
        with conn.cursor() as cur:
            # заполнение таблицы customers
            with open('north_data/customers_data.csv', 'r', encoding='utf-8', newline='') as file:
                customers = csv.DictReader(file)
                for cust in customers:
                    cur.execute(
                        'INSERT INTO customers VALUES (%s, %s, %s)',
                        (cust["customer_id"], cust["company_name"], cust["contact_name"])
                    )
            # заполнение таблицы employees
            with open('north_data/employees_data.csv', 'r', encoding='utf-8', newline='') as file:
                employees = csv.DictReader(file)
                for emp in employees:
                    cur.execute(
                        'INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                        (emp["employee_id"], emp["first_name"], emp["last_name"], emp["title"], emp["birth_date"], emp["notes"])
                    )
            # заполнение таблицы orders
            with open('north_data/orders_data.csv', 'r', encoding='utf-8', newline='') as file:
                orders = csv.DictReader(file)
                for order in orders:
                    cur.execute(
                        'INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                        (order["order_id"], order["customer_id"], order["employee_id"], order["order_date"], order["ship_city"])
                    )

finally:
    # закрытие соединения с БД
    conn.close()
