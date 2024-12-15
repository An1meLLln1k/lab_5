import pymongo
import pandas as pd
import json

# Подключение к MongoDB
client = pymongo.MongoClient()
db = client["db-2024"]
collection = db["jobs"]

# Считывание данных из нового файла (например, CSV)
new_file_path = 'task_2_item.csv'  # Замените на путь к вашему новому файлу
new_data = pd.read_csv(new_file_path)

# Преобразуем данные в список словарей, которые можно вставить в MongoDB
new_data_dict = new_data.to_dict(orient="records")

# Вставка новых данных в коллекцию
collection.insert_many(new_data_dict)
print("Новые данные успешно добавлены в MongoDB!")

# Запрос на статистику по зарплатам
pipeline_salary = [
    {
        "$group": {
            "_id": None,
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }
    }
]

salary_stats = list(collection.aggregate(pipeline_salary))
with open('salary_stats.json', 'w', encoding='utf-8') as f:
    json.dump(salary_stats, f, ensure_ascii=False, indent=4)

# Запрос на количество документов по профессиям
pipeline_professions = [
    {
        "$group": {
            "_id": "$job",  # Группировка по профессиям
            "count": {"$sum": 1}  # Подсчёт количества
        }
    },
    {"$sort": {"count": -1}}  # Сортировка по убыванию количества
]

profession_count = list(collection.aggregate(pipeline_professions))
with open('profession_count.json', 'w', encoding='utf-8') as f:
    json.dump(profession_count, f, ensure_ascii=False, indent=4)

# Запрос на статистику по зарплатам для каждого города
pipeline_city_salary = [
    {
        "$group": {
            "_id": "$city",  # Группировка по городу
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }
    }
]

city_salary_stats = list(collection.aggregate(pipeline_city_salary))
with open('city_salary_stats.json', 'w', encoding='utf-8') as f:
    json.dump(city_salary_stats, f, ensure_ascii=False, indent=4)

# Запрос на статистику по зарплатам для каждой профессии
pipeline_profession_salary = [
    {
        "$group": {
            "_id": "$job",  # Группировка по профессии
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }
    }
]

profession_salary_stats = list(collection.aggregate(pipeline_profession_salary))
with open('profession_salary_stats.json', 'w', encoding='utf-8') as f:
    json.dump(profession_salary_stats, f, ensure_ascii=False, indent=4)

# Запрос на статистику по возрасту для каждого города
pipeline_city_age = [
    {
        "$group": {
            "_id": "$city",  # Группировка по городу
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }
    }
]

city_age_stats = list(collection.aggregate(pipeline_city_age))
with open('city_age_stats.json', 'w', encoding='utf-8') as f:
    json.dump(city_age_stats, f, ensure_ascii=False, indent=4)

# Запрос на статистику по возрасту для каждой профессии
pipeline_profession_age = [
    {
        "$group": {
            "_id": "$job",  # Группировка по профессии
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }
    }
]

profession_age_stats = list(collection.aggregate(pipeline_profession_age))
with open('profession_age_stats.json', 'w', encoding='utf-8') as f:
    json.dump(profession_age_stats, f, ensure_ascii=False, indent=4)

# Запрос на максимальную зарплату при минимальном возрасте
pipeline_max_salary_min_age = [
    {
        "$group": {
            "_id": "$age",  # Группировка по возрасту
            "max_salary": {"$max": "$salary"}
        }
    },
    {"$sort": {"_id": 1}},  # Сортировка по возрасту
    {"$limit": 1}  # Получение минимального возраста
]

max_salary_min_age = list(collection.aggregate(pipeline_max_salary_min_age))
with open('max_salary_min_age.json', 'w', encoding='utf-8') as f:
    json.dump(max_salary_min_age, f, ensure_ascii=False, indent=4)

# Запрос для максимальной зарплаты при минимальном возрасте
pipeline_max_salary_min_age = [
    # Сортировка по возрасту
    {"$sort": {"age": 1}},  # Сортировка по возрасту по возрастанию

    # Группировка по минимальному возрасту
    {
        "$group": {
            "_id": None,  # Мы не группируем по возрасту, так как нам нужен минимальный возраст
            "min_age": {"$min": "$age"},  # Находим минимальный возраст
            "max_salary": {"$max": "$salary"}  # Находим максимальную зарплату
        }
    }
]

# Выполнение запроса
max_salary_min_age = list(collection.aggregate(pipeline_max_salary_min_age))

# Сохранение результата в JSON
with open('max_salary_min_age.json', 'w', encoding='utf-8') as f:
    json.dump(max_salary_min_age, f, ensure_ascii=False, indent=4)

print("Результат сохранен в max_salary_min_age.json")


# Запрос на минимальный, средний, максимальный возраст по городу с зарплатой > 50,000
pipeline_age_salary_50k = [
    {"$match": {"salary": {"$gt": 50000}}},  # Фильтрация по зарплате
    {
        "$group": {
            "_id": "$city",  # Группировка по городу
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }
    },
    {"$sort": {"avg_age": -1}}  # Сортировка по убыванию по avg_age
]

age_salary_50k_stats = list(collection.aggregate(pipeline_age_salary_50k))
with open('age_salary_50k_stats.json', 'w', encoding='utf-8') as f:
    json.dump(age_salary_50k_stats, f, ensure_ascii=False, indent=4)

# Запрос на минимальную, среднюю, максимальную зарплату в заданных возрастных диапазонах
pipeline_salary_range = [
    # $match: фильтрация по возрасту с использованием $or
    {
        "$match": {
            "$or": [
                {"age": {"$gt": 18, "$lt": 25}},  # Возраст 18-25
                {"age": {"$gt": 50, "$lt": 65}}   # Возраст 50-65
            ]
        }
    },

    # $group: группировка по городу и профессии
    {
        "$group": {
            "_id": {"city": "$city", "job": "$job"},  # Группировка по городу и профессии
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }
    }
]

# Выполнение запроса
salary_range_stats = list(collection.aggregate(pipeline_salary_range))

# Сохранение результата в JSON
with open('salary_range_stats.json', 'w', encoding='utf-8') as f:
    json.dump(salary_range_stats, f, ensure_ascii=False, indent=4)

print("Результат сохранен в salary_range_stats.json")


# Произвольный запрос с $match, $group, $sort
pipeline_custom = [
    # $match: фильтруем людей с зарплатой больше 50,000
    {
        "$match": {
            "salary": {"$gt": 50000}
        }
    },
    # $group: группируем по профессии и считаем минимальную, среднюю и максимальную зарплату
    {
        "$group": {
            "_id": "$job",  # Группируем по профессии
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }
    },
    # $sort: сортируем по максимальной зарплате в профессии (по убыванию)
    {
        "$sort": {
            "max_salary": -1  # По убыванию
        }
    }
]

# Выполняем запрос
custom_stats = list(collection.aggregate(pipeline_custom))

# Сохраняем результат в JSON
with open('custom_stats.json', 'w', encoding='utf-8') as f:
    json.dump(custom_stats, f, ensure_ascii=False, indent=4)

print("Результат произвольного запроса сохранен в custom_stats.json")




