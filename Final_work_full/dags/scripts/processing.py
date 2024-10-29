import random
from datetime import date, timedelta
import pandas as pd


def create_uid():
    '''Создает один уникальный идентификатор из 20 символов'''
    return ''.join([random.choice('qwertyuiopasdfghjklzxcvbnm0123456789') for _ in range(20)])


def get_random_sales(number_of_customers = 500,
                    number_of_products = 100,
                    number_of_sales = 1000000):
    '''Создает случайные данные, согласно формата задания, у которых гарантировано нет двух одинаковых строк.
    Стандартные настройки:
    Количество пользователей = 500;
    Количество продуктов = 100;
    Количество продаж = 1 000 000'''

    # Создаем уникальные идентификаторы для пользователей, продуктов и продаж
    all_id = set()
    while len(all_id) < number_of_customers + number_of_products + number_of_sales:
        all_id.add(create_uid())

    # Создаем кортеж уникальных пользователей
    customers = set()
    while len(customers) < number_of_customers:
        customers.add(all_id.pop())
    customers = tuple(customers)

    # Создаем словарь уникальных продуктов с их ценами
    products = dict()
    while len(products) < number_of_products:
        products[all_id.pop()] = random.randrange(10, 10001, 10)

    # Создаем множество уникальных идентификаторов продаж
    sales = all_id

    # Создаем список дат продаж
    sale_dates = [date.today() - timedelta(days=i) for i in range(365)]

    # Определяем регионы, в которых происходят продажи
    moscow_regions = ['Центральный',
                    'Северный',
                    'Северо-Восточный',
                    'Восточный',
                    'Юго-Восточный',
                    'Южный',
                    'Юго-Западный',
                    'Западный',
                    'Северо-Западный',
                    'Зеленоградский',
                    'Новомосковский',
                    'Троицкий']

    # Создаем список продаж
    sales_list = list()
    while sales:
        sale_data = list() # Создаем список
        sale_data.append(sales.pop()) # Уникальный идентификатор продажи
        sale_data.append(random.choice(customers)) # Уникальный идентификатор пользователя
        sale_data.append(random.choice(list(products.keys()))) # Уникальный идентификатор продукта
        sale_data.append(random.randint(1, 10)) # Количество товара
        sale_data.append(random.choice(sale_dates)) # Дата продажи
        sale_data.append(sale_data[3] * products[sale_data[2]]) # Сумма продажи
        sale_data.append(random.choice(moscow_regions)) # Регион

        sales_list.append(sale_data) # Добавляем новую продажу в список продаж

    # Возвращаем сгенерированный список продаж
    return sales_list