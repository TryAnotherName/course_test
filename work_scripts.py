# Импортируем нужные либы
from datetime import datetime
import matplotlib.pyplot as plt

# Создаем требуемые функции
def read_sales_data(file_path):
    '''Читает файл по указанному пути'''
    data = list()
    with open(file_path, 'r', encoding='UTF') as file:
        for line_data in file.readlines():

            line = dict()
            line_data = line_data.replace('\n', '').split(', ')
            line['product_name'] = line_data[0]
            line['quantity'] = int(line_data[1])
            line['price'] = int(line_data[2])
            line['date'] = datetime.strptime(line_data[3], '%Y-%m-%d').date()
            data.append(line)

    return data

def total_sales_per_product(sales_data):
    '''Вычисляет продажи товара'''
    total_sales = dict()
    for sale in sales_data:
        if sale['product_name'] not in total_sales:
            total_sales[sale['product_name']] = sale['quantity'] * sale['price']
        else:
            total_sales[sale['product_name']] += sale['quantity'] * sale['price']

    return total_sales

def sales_over_time(sales_data):
    '''Вычисляет продажи по дням'''
    total_sales = dict()
    for sale in sales_data:
        if sale['date'] not in total_sales:
            total_sales[sale['date']] = sale['quantity'] * sale['price']
        else:
            total_sales[sale['date']] += sale['quantity'] * sale['price']
    total_sales = dict(sorted(total_sales.items()))
    return total_sales

def show_graf(data_dict, x_lable='X', y_label='Y', title='Какой то график'):
    '''Выводит график на экран'''
    plt.figure(figsize=(10, 5))
    plt.plot(data_dict.keys(), data_dict.values())
    plt.xlabel(x_lable)
    plt.ylabel(y_label)
    plt.title(title)
    plt.show()