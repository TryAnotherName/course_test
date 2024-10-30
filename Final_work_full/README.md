![Python Version](https://img.shields.io/badge/python-3.8.10-blue)

## Задание
* Сгенерировать данные о 1 миллионе продаж за последний год.
* Каждая запись о продаже должна содержать следующие поля:
    ```
    - sale_id (уникальный идентификатор продажи).
    - customer_id (идентификатор клиента).
    - product_id (идентификатор продукта).
    - quantity (количество купленных товаров).
    - sale_date (дата продажи).
    - sale_amount (сумма продажи, рассчитывается как количество товаров * случайная цена товара).
    - region (регион клиента, один из: North, South, East, West).```
* Удалить дубликаты записей о продажах.
* Привести данные к нужным форматам для дальнейшей обработки.
* Создать таблицу для хранения данных о продажах в PostgreSQL.
* Вставить очищенные данные в эту таблицу.
* Выполнить агрегацию данных, используя оконные функции и группировки:
    * Подсчитать общее количество продаж и сумму продаж для каждого региона и каждого продукта.
    * Рассчитать средний чек (average_sale_amount) по регионам и продуктам.
    * Сохранить агрегированные данные в отдельную таблицу в PostgreSQL.
* Перенести агрегированные данные из PostgreSQL в ClickHouse. Добавить дату импорта.

## Установка
1. Клонируйте репозиторий:
    ```bash
    git clone https://github.com/TryAnotherName/course_test.git
    ```
2. Перейдите в папку с заданием и создайте папки, необходимые для работы Airflow:
    ```bash
    cd Final_work_full
    mkdir config
    mkdir data
    mkdir logs
    mkdir plugins
    ```
3. Запустите контейнер:
    ```bash
    docker-compose up -d
    ```

## Описание файлов и папок
* `docker-compose.yml` - контейнер из курса, предоставленный для выполнения заданий;
* `dags` - папка, в которой хранятся ДАГи;
* `dags/scripts/processing.py` - содержит функции, необходимые для генерации данных. Вынесены из основного файла ДАГа, чтобы не захломлять его.
* `dags/main.py` - основной файл ДАГа.


## Создание подключений
В Airflow необходимо создать 2 подключения:
1. `clickhouse_connection`
2. `postgres_connection`

### clickhouse_connection
* Connection Id - clickhouse_connection
* Connection Type - Sqlite
* Host - host.docker.internal
* Port - 9000

Поля Login и Password оставить пустыми!

### postgres_connection
* Connection Id - postgres_connection
* Connection Type - Postgres
* Host - host.docker.internal
* Login - user
* Password - password
* Port - 5432

## Контакты для связи
`Telegram` - https://t.me/tryanothername14