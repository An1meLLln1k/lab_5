import jsonlines
from bson import json_util

# Загрузка данных из JSON с использованием jsonlines
def get_data_from_json_with_bson(file_name):
    data = []
    with jsonlines.open(file_name) as reader:
        for obj in reader:
            # С использованием json_util обрабатываем объект, если он содержит ObjectId
            data.append(json_util.loads(json_util.dumps(obj)))  # Сериализуем и десериализуем с json_util
    return data

# Укажите путь к вашему JSON файлу
file_name = 'supermarket_sales_part2.json'

# Сохранение данных из JSON в переменную
data = get_data_from_json_with_bson(file_name)

# Печать данных
print(data)
