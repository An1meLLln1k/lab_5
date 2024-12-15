import pymongo
import json
from bson import ObjectId
import pandas as pd

# Кастомный сериализатор для преобразования ObjectId в строку
def json_converter(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

# Функция для загрузки данных в MongoDB
def load_data_to_mongo(file_path):
    # Подключение к MongoDB
    client = pymongo.MongoClient()
    db = client["db-2024"]  # База данных
    collection = db["jobs"]  # Коллекция, куда будем сохранять данные

    # Чтение данных из файла
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Вставка данных в MongoDB
    collection.insert_many(data)
    print("Данные успешно загружены в MongoDB!")


# Пример использования
load_data_to_mongo('task_1_item.json')  # Замените на путь к вашему файлу

# Функция для запроса топ-10 по зарплате
def query_top_10_by_salary():
    # Подключение к MongoDB
    client = pymongo.MongoClient()
    db = client["db-2024"]
    collection = db["jobs"]

    # Запрос: первые 10 записей, отсортированные по убыванию по полю salary
    top_10_salary = collection.find().sort("salary", pymongo.DESCENDING).limit(10)

    # Преобразование результата в список и сохранение результата в JSON
    result = list(top_10_salary)
    for doc in result:
        doc['_id'] = str(doc['_id'])  # Преобразуем ObjectId в строку

    # Сохранение результата в JSON
    with open('top_10_salary.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print("Результаты сохранены в top_10_salary.json")

# Пример использования
query_top_10_by_salary()

# Функция для запроса молодых специалистов с высокой зарплатой
def query_young_and_high_salary():
    # Подключение к MongoDB
    client = pymongo.MongoClient()
    db = client["db-2024"]
    collection = db["jobs"]

    # Запрос: фильтрация по age < 30, сортировка по убыванию salary
    young_high_salary = collection.find({"age": {"$lt": 30}}).sort("salary", pymongo.DESCENDING).limit(15)

    # Преобразование результата в список и сохранение результата в JSON
    result = list(young_high_salary)
    for doc in result:
        doc['_id'] = str(doc['_id'])  # Преобразуем ObjectId в строку

    # Сохранение результата в JSON
    with open('young_high_salary.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print("Результаты сохранены в young_high_salary.json")

# Пример использования
query_young_and_high_salary()

# Функция для запроса с комплексным фильтром по городу и профессиям
def query_complex_filter():
    # Подключение к MongoDB
    client = pymongo.MongoClient()
    db = client["db-2024"]
    collection = db["jobs"]

    # Пример произвольного города и профессий
    city = "Мадрид"  # или любой другой город
    professions = ["Врач", "Программист", "Повар"]  # произвольные профессии

    # Запрос: фильтрация по городу и профессиям, сортировка по возрасту
    complex_filter = collection.find({
        "city": city,
        "job": {"$in": professions}
    }).sort("age", pymongo.ASCENDING).limit(10)

    # Преобразование результата в список и сохранение результата в JSON
    result = list(complex_filter)
    for doc in result:
        doc['_id'] = str(doc['_id'])  # Преобразуем ObjectId в строку

    # Сохранение результата в JSON
    with open('complex_filter_result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print("Результаты сохранены в complex_filter_result.json")

# Пример использования
query_complex_filter()

def query_filtered_count():
    # Подключение к MongoDB
    client = pymongo.MongoClient()
    db = client["db-2024"]
    collection = db["jobs"]

    # Пример диапазона возрастов и зарплат
    age_range = {"$gte": 25, "$lte": 50}  # Произвольный диапазон
    salary_range_1 = {"$gt": 50000, "$lte": 75000}
    salary_range_2 = {"$gt": 125000, "$lt": 150000}

    # Запрос: фильтрация по возрасту, году и зарплате
    filtered_count = collection.count_documents({
        "age": age_range,
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": salary_range_1},
            {"salary": salary_range_2}
        ]
    })

    # Сохранение результата в JSON
    result = {"count": filtered_count}
    with open('filtered_count.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print("Результат сохранён в filtered_count.json")


# Пример использования
query_filtered_count()
