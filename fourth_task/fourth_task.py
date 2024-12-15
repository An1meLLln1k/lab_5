import pymongo
import jsonlines
import pandas as pd
import json
from bson import json_util
from pymongo import MongoClient
from bson import ObjectId

# Подключение к MongoDB
client = MongoClient()
db = client["supermarket_db"]
collection = db["sales_data"]


# Загрузка данных из JSON с использованием jsonlines
def get_data_from_json_with_bson(file_name):
    data = []
    with jsonlines.open(file_name) as reader:
        for obj in reader:
            # С использованием json_util обрабатываем объект, если он содержит ObjectId
            data.append(json_util.loads(json_util.dumps(obj)))  # Сериализуем и десериализуем с json_util
    return data


# Загрузка данных из CSV
def get_data_from_csv(file_name):
    data = list()
    df = pd.read_csv(file_name)
    for _, row in df.iterrows():
        item = {
            "Unit price": float(row['Unit price']),
            "Quantity": int(row['Quantity']),
            "Tax 5%": float(row['Tax 5%']),
            "Product line": row['Product line'],
            "City": row['City'],
            "Branch": row['Branch'],
            "Gender": row['Gender'],
            "Customer type": row['Customer type'],
            "Invoice ID": row['Invoice ID']
        }
        data.append(item)
    return data


# Файлы для загрузки
csv_file = 'supermarket_sales_part1.csv'  # Укажите путь к вашему CSV файлу
json_file = 'supermarket_sales_part2.json'  # Укажите путь к вашему JSON файлу

# Получаем данные
data_csv = get_data_from_csv(csv_file)
data_json = get_data_from_json_with_bson(json_file)

# Вставка данных в коллекцию MongoDB
collection.insert_many(data_csv)
collection.insert_many(data_json)

print("Данные из CSV и JSON успешно загружены в MongoDB.")


# Функция для обработки ObjectId в строку
def convert_objectid_to_str(doc):
    if isinstance(doc, dict):
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                doc[key] = str(value)  # Преобразуем ObjectId в строку
            elif isinstance(value, dict):
                convert_objectid_to_str(value)
            elif isinstance(value, list):
                for item in value:
                    convert_objectid_to_str(item)
    return doc


# 1. Вывод первых 10 записей, отсортированных по убыванию по полю Unit price
def get_top_10_by_unit_price():
    top_10 = list(collection.find().sort("Unit price", pymongo.DESCENDING).limit(10))

    # Преобразуем ObjectId в строку перед сериализацией
    top_10 = [convert_objectid_to_str(item) for item in top_10]

    # Сохранение результата в JSON
    with open('top_10_by_unit_price.json', 'w', encoding='utf-8') as f:
        json.dump(top_10, f, ensure_ascii=False, indent=4)


# 2. Вывод первых 15 записей, отфильтрованных по предикату Unit price > 20, отсортированных по убыванию по полю quantity
def get_filtered_by_price_and_quantity():
    filtered = list(collection.find({"Unit price": {"$gt": 20}}).sort("quantity", pymongo.DESCENDING).limit(15))

    # Преобразуем ObjectId в строку перед сериализацией
    filtered = [convert_objectid_to_str(item) for item in filtered]

    # Сохранение результата в JSON
    with open('filtered_by_price_and_quantity.json', 'w', encoding='utf-8') as f:
        json.dump(filtered, f, ensure_ascii=False, indent=4)


# 3. Вывод первых 10 записей, отфильтрованных по сложному предикату: город и product line, отсортированных по возрастанию по полю Tax 5%
def get_filtered_by_city_and_product_line():
    # Фильтрация по городу и product line
    complex_filter = {
        "City": "Yangon",  # Произвольный город
        "Product line": {"$in": ["Sports travel", "Electronic accessories"]}  # Произвольные product lines
    }

    # Получаем отфильтрованные записи
    filtered = list(collection.find(complex_filter).sort("Tax 5%", pymongo.ASCENDING).limit(10))

    # Преобразуем ObjectId в строку перед сериализацией
    filtered = [convert_objectid_to_str(item) for item in filtered]

    # Сохранение результата в JSON
    with open('filtered_by_city_and_product_line.json', 'w', encoding='utf-8') as f:
        json.dump(filtered, f, ensure_ascii=False, indent=4)


# Вызов функции
get_filtered_by_city_and_product_line()


# 4. Вывод первых 10 записей, отсортированных по убыванию по полю Tax 5% и только из города Yangon
def get_filtered_by_tax_and_city():
    filtered = list(collection.find({"City": "Yangon"}).sort("Tax 5%", pymongo.DESCENDING).limit(10))

    # Преобразуем ObjectId в строку перед сериализацией
    filtered = [convert_objectid_to_str(item) for item in filtered]

    # Сохранение результата в JSON
    with open('filtered_by_tax_and_city.json', 'w', encoding='utf-8') as f:
        json.dump(filtered, f, ensure_ascii=False, indent=4)


# 5. Вывод количества записей, получаемых в результате фильтрации по диапазонам Tax 5%, quantity и Unit price
def get_filtered_count():
    filter_condition = {
        "Tax 5%": {"$gte": 5, "$lte": 50},
        "$or": [
            {"quantity": {"$gte": 1, "$lte": 5}},
            {"Unit price": {"$gt": 10, "$lt": 40}}
        ]
    }

    # Подсчет документов
    count = collection.count_documents(filter_condition)
    result = {"count": count}

    # Сохранение результата в JSON
    with open('filtered_count.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

# Выполнение запроса
get_filtered_count()

# 6. Вывод минимальной, средней, максимальной Unit price по всей коллекции
def get_unit_price_stats():
    """
    Запрос 6: Вывод минимальной, средней, максимальной Unit price по всей коллекции.
    Сохраняем результат в JSON файл.
    """
    pipeline = [
        {
            "$group": {
                "_id": "Unit price stats",  # Объединяем все документы в один
                "min_unit_price": {"$min": "$Unit price"},  # Минимальное значение Unit price
                "avg_unit_price": {"$avg": "$Unit price"},  # Среднее значение Unit price
                "max_unit_price": {"$max": "$Unit price"}  # Максимальное значение Unit price
            }
        }
    ]
    result = list(collection.aggregate(pipeline))

    # Сохранение результата в JSON
    with open('unit_price_stats.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

# 7. Вывод количества данных по представленным Product line
def get_product_line_count():
    """
    Запрос 7: Вывод количества данных по каждой категории Product line.
    Сохраняем результат в JSON файл.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$Product line",  # Группируем по полю Product line
                "count": {"$sum": 1}  # Подсчитываем количество документов в каждой группе
            }
        },
        {
            "$sort": {"count": -1}  # Сортируем по убыванию количества
        }
    ]
    result = list(collection.aggregate(pipeline))

    # Сохранение результата в JSON
    with open('product_line_count.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

# 8. Вывод минимальной, средней, максимальной Unit price по городу
def get_unit_price_by_city():
    """
    Запрос 8: Вывод минимальной, средней, максимальной Unit price по каждому городу.
    Сохраняем результат в JSON файл.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$City",  # Группируем по городу
                "min_unit_price": {"$min": "$Unit price"},
                "avg_unit_price": {"$avg": "$Unit price"},
                "max_unit_price": {"$max": "$Unit price"}
            }
        }
    ]
    result = list(collection.aggregate(pipeline))

    # Сохранение результата в JSON
    with open('unit_price_by_city.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

# 9. Вывод минимальной, средней, максимальной Unit price по Branch
def get_unit_price_by_branch():
    """
    Запрос 9: Вывод минимальной, средней, максимальной Unit price по каждому Branch.
    Сохраняем результат в JSON файл.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$Branch",  # Группируем по филиалу (Branch)
                "min_unit_price": {"$min": "$Unit price"},
                "avg_unit_price": {"$avg": "$Unit price"},
                "max_unit_price": {"$max": "$Unit price"}
            }
        }
    ]
    result = list(collection.aggregate(pipeline))

    # Сохранение результата в JSON
    with open('unit_price_by_branch.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

# 10. Вывод минимального, среднего, максимального Tax 5% по Product line
def get_tax_by_product_line():
    """
    Запрос 10: Вывод минимального, среднего, максимального Tax 5% по каждому Product line.
    Сохраняем результат в JSON файл.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$Product line",  # Группируем по полю Product line
                "min_tax": {"$min": "$Tax 5%"},
                "avg_tax": {"$avg": "$Tax 5%"},
                "max_tax": {"$max": "$Tax 5%"}
            }
        }
    ]
    result = list(collection.aggregate(pipeline))

    # Сохранение результата в JSON
    with open('tax_by_product_line.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

# 11. Удалить из коллекции документы по предикату: Unit price < 10 || Unit price > 80
def delete_by_unit_price():
    """
    Запрос 11: Удалить из коллекции документы по предикату:
    Unit price < 10 || Unit price > 80.
    """
    result = collection.delete_many({
        "$or": [
            {"Unit price": {"$lt": 10}},
            {"Unit price": {"$gt": 80}}
        ]
    })
    result_data = {"deleted_count": result.deleted_count}

    # Сохранение результата в JSON
    with open('delete_by_unit_price.json', 'w', encoding='utf-8') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=4)

# 12. Увеличить (Tax 5%) всех документов на 1%
def increase_tax_by_1_percent():
    """
    Запрос 12: Увеличить (Tax 5%) всех документов на 1%.
    """
    result = collection.update_many(
        {},
        {"$mul": {"Tax 5%": 1.01}}  # Увеличиваем Tax 5% на 1%
    )
    result_data = {"modified_count": result.modified_count}

    # Сохранение результата в JSON
    with open('increase_tax_by_1_percent.json', 'w', encoding='utf-8') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=4)

# 13. Поднять Unit price на 10% для произвольно выбранных городов
def increase_unit_price_for_cities():
    """
    Запрос 13: Поднять Unit price на 10% для произвольно выбранных городов.
    """
    cities = ["Yangon", "Mandalay", "Naypyitaw"]  # Пример произвольных городов
    result = collection.update_many(
        {"City": {"$in": cities}},  # Применяем для выбранных городов
        {"$mul": {"Unit price": 1.10}}  # Увеличиваем Unit price на 10%
    )
    result_data = {"modified_count": result.modified_count}

    # Сохранение результата в JSON
    with open('increase_unit_price_for_cities.json', 'w', encoding='utf-8') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=4)

# 14. Поднять заработную плату на 15% для выборки по сложному предикату
def increase_salary_for_complex_filter():
    """
    Запрос 14: Поднять заработную плату на 15% для выборки по сложному предикату:
    Произвольный город, произвольный Product line, произвольный диапазон Branch.
    """
    complex_filter = {
        "City": "Yangon",  # Произвольный город
        "Product line": {"$in": ["Consumer", "Home", "Health and beauty"]},  # Произвольные product lines
        "Branch": {"$in": ["A", "B"]}  # Произвольные Branch
    }
    result = collection.update_many(
        complex_filter,
        {"$mul": {"Unit price": 1.15}}  # Увеличиваем Unit price на 15%
    )
    result_data = {"modified_count": result.modified_count}

    # Сохранение результата в JSON
    with open('increase_salary_for_complex_filter.json', 'w', encoding='utf-8') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=4)

# 15. Удалить из коллекции записи по произвольному предикату
def delete_by_custom_predicate():
    """
    Запрос 15: Удалить из коллекции записи по произвольному предикату.
    Пример: Удаление записей для определенного города и Product line.
    """
    result = collection.delete_many({
        "City": "Yangon",
        "Product line": {"$in": ["Fashion accessories", "Office"]}
    })
    result_data = {"deleted_count": result.deleted_count}

    # Сохранение результата в JSON
    with open('delete_by_custom_predicate.json', 'w', encoding='utf-8') as f:
        json.dump(result_data, f, ensure_ascii=False, indent=4)

# Выполнение запросов
get_top_10_by_unit_price() # Запрос 1
get_filtered_by_price_and_quantity() # Запрос 2
get_filtered_by_city_and_product_line() # Запрос 3
get_filtered_by_tax_and_city() # Запрос 4
get_filtered_count() # Запрос 5
get_unit_price_stats()  # Запрос 6
get_product_line_count()  # Запрос 7
get_unit_price_by_city()  # Запрос 8
get_unit_price_by_branch()  # Запрос 9
get_tax_by_product_line()  # Запрос 10
delete_by_unit_price()  # Запрос 11
increase_tax_by_1_percent()  # Запрос 12
increase_unit_price_for_cities()  # Запрос 13
increase_salary_for_complex_filter()  # Запрос 14
delete_by_custom_predicate()  # Запрос 15