# Импортируем написанные нами функции из отдельного .py файла
from work_scripts import *

# Загружаем данные из нашего файла
sales = read_sales_data('data/dataset.csv')

# Обрабатываем данные продаж по типу товара и выводим график
product_sales = total_sales_per_product(sales)
best_product_name = max(product_sales, key=product_sales.get)
print(f'Продукт {best_product_name} показал наибольшую выручку, которая составила {product_sales[best_product_name]} миллионов долларов')
show_graf(product_sales, 'Тип товара', 'Сумма продаж, млн. долл.', 'Продажи по типам товара')

# Обрабатываем данные продаж по датам и выводим график
date_sales = sales_over_time(sales)
best_day_name = max(date_sales, key=date_sales.get)
print(f'{best_day_name} были зафиксированы максимальные продажи, которые составили {date_sales[best_day_name]} миллионов долларов')
show_graf(date_sales, 'Дата', 'Сумма продаж, млн. долл.', 'Продажи по датам')