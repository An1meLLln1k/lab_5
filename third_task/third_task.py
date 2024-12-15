import pymongo
import pandas as pd
import json

# Подключение к MongoDB
client = pymongo.MongoClient()
db = client["db-2024"]
collection = db["jobs"]

# Загрузка данных из файла pickle
file_path = 'task_3_item.pkl'  # Укажите путь к файлу
data = pd.read_pickle(file_path)

# Проверяем тип данных
if isinstance(data, pd.DataFrame):  # Если данные в формате DataFrame
    # Преобразуем DataFrame в список словарей
    data_dict = data.to_dict(orient="records")
elif isinstance(data, list):  # Если данные уже в формате списка
    data_dict = data  # Просто присваиваем
else:
    print("Невозможно обработать данные. Тип данных:", type(data))
    exit()

# Вставка новых данных в коллекцию
collection.insert_many(data_dict)
print("Новые данные успешно добавлены в MongoDB!")

# Запрос на удаление документов с зарплатой меньше 25 000 или больше 175 000
result_delete_salary = collection.delete_many({
    "$or": [
        {"salary": {"$lt": 25000}},  # Зарплата меньше 25000
        {"salary": {"$gt": 175000}}   # Зарплата больше 175000
    ]
})

# Сохранение результатов в JSON
delete_salary_stats = {"deleted_count": result_delete_salary.deleted_count}
with open('deleted_salary.json', 'w', encoding='utf-8') as f:
    json.dump(delete_salary_stats, f, ensure_ascii=False, indent=4)

print("Результат удаления записей сохранен в deleted_salary.json")

# Запрос на увеличение возраста на 1
result_age_increment = collection.update_many(
    {},  # Применяем ко всем документам
    {"$inc": {"age": 1}}  # Увеличиваем поле age на 1
)

# Сохранение результатов в JSON
age_increment_stats = {"modified_count": result_age_increment.modified_count}
with open('age_increment.json', 'w', encoding='utf-8') as f:
    json.dump(age_increment_stats, f, ensure_ascii=False, indent=4)

print("Результат увеличения возраста сохранен в age_increment.json")

# Пример списка профессий для повышения зарплаты
selected_jobs = ["Программист", "Менеджер"]

# Запрос на повышение зарплаты на 5% для выбранных профессий
result_salary_increase_job = collection.update_many(
    {"job": {"$in": selected_jobs}},  # Фильтрация по профессиям
    {"$mul": {"salary": 1.05}}  # Увеличиваем зарплату на 5%
)

# Сохранение результатов в JSON
salary_increase_job_stats = {"modified_count": result_salary_increase_job.modified_count}
with open('salary_increase_job.json', 'w', encoding='utf-8') as f:
    json.dump(salary_increase_job_stats, f, ensure_ascii=False, indent=4)

print("Результат повышения зарплаты для профессий сохранен в salary_increase_job.json")

# Пример списка городов для повышения зарплаты
selected_cities = ["Москва", "Санкт-Петербург"]

# Запрос на повышение зарплаты на 7% для выбранных городов
result_salary_increase_city = collection.update_many(
    {"city": {"$in": selected_cities}},  # Фильтрация по городам
    {"$mul": {"salary": 1.07}}  # Увеличиваем зарплату на 7%
)

# Сохранение результатов в JSON
salary_increase_city_stats = {"modified_count": result_salary_increase_city.modified_count}
with open('salary_increase_city.json', 'w', encoding='utf-8') as f:
    json.dump(salary_increase_city_stats, f, ensure_ascii=False, indent=4)

print("Результат повышения зарплаты для городов сохранен в salary_increase_city.json")

# Пример запроса на повышение зарплаты на 10% для выборки по сложному предикату
complex_filter = {
    "city": {"$in": ["Москва", "Санкт-Петербург", "Екатеринбург"]},  # Несколько городов
    "job": {"$in": ["Программист", "Менеджер", "Аналитик"]},  # Несколько профессий
    "age": {"$gte": 25, "$lte": 45}  # Диапазон возраста от 25 до 45 лет
}

# Запрос на повышение зарплаты на 10% для выборки по сложному предикату
result_salary_increase_complex = collection.update_many(
    complex_filter,  # Применяем фильтр
    {"$mul": {"salary": 1.10}}  # Увеличиваем зарплату на 10%
)

# Выводим, сколько документов было изменено
print(f"Количество изменённых документов: {result_salary_increase_complex.modified_count}")

# Сохранение результатов в JSON
salary_increase_complex_stats = {"modified_count": result_salary_increase_complex.modified_count}
with open('salary_increase_complex.json', 'w', encoding='utf-8') as f:
    json.dump(salary_increase_complex_stats, f, ensure_ascii=False, indent=4)

print("Результат повышения зарплаты по сложному предикату сохранен в salary_increase_complex.json")


# Произвольный предикат: удаляем записи с профессией "Менеджер"
delete_predicate_filter = {"job": "Менеджер"}  # Удаляем записи с профессией "Менеджер"

# Выполнение удаления
delete_predicate_result = collection.delete_many(delete_predicate_filter)

# Сохранение результата в JSON
delete_predicate_stats = {"deleted_count": delete_predicate_result.deleted_count}
with open('deleted_predicate.json', 'w', encoding='utf-8') as f:
    json.dump(delete_predicate_stats, f, ensure_ascii=False, indent=4)

print(f"Удалено {delete_predicate_result.deleted_count} документов по заданному предикату.")

