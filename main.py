'''
Вопрос №3:
Задание:

В PySpark приложении в виде DataFrame заданы продукты, категории и связи между ними. 
Каждому продукту может соответствовать несколько категорий или ни одной. 
А каждой категории может соответствовать несколько продуктов или ни одного. 
Напишите метод на PySpark, который в одном 
DataFrame вернет все пары «Имя продукта – Имя категории» и имена всех продуктов, 
у которых нет категорий.
'''





import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, when

os.environ['PYSPARK_PYTHON'] = './venv/Scripts/python'
spark = SparkSession.builder \
    .master('local') \
    .appName('PySpark Tutorial') \
    .getOrCreate()

#(product_id, categoty_id)
product_category_data = [
    (1, 1),
    (2, 1),
    (3, 2),
]
#(product_id, product_title, product_price)
product_data = [
    (1, 'Nike Air 90s', 4999),
    (2, 'Jordan 5', 12000),
    (3, 'Nike Sportswear Windrunner', 1000),
    (4, 'NonameProduct', 999),
]

#(category_id, category_title)
category_data = [
    (1, 'shoe'),
    (2, 'jaket'),
]

# Допустим существуют готовые dataframes о продуктах, категориях и их связях
product_category_DF = spark.createDataFrame(product_category_data, ['product_id', 'category_id'])
product_DF = spark.createDataFrame(product_data, ['id', 'title', 'price'])
category_DF = spark.createDataFrame(category_data, ['id', 'title'])

# метод возвращающий в одном DataFrame пары 'Имя продукта'-'Имя категории', а так же имена продуктов без категории (Category is Null)
def get_result_df(product_df, category_df, product_category_df):
        result_df = product_df.join(product_category_df, product_df.id == product_category_df.product_id, 'left') \
                        .join(category_df, product_category_df.category_id == category_df.id, 'left') \
                        .select(product_df.title.alias("Имя продукта"), category_df.title.alias("Имя категории"))
        return result_df

result_df = get_result_df(product_DF, category_DF, product_category_DF)
result_df.show()

'''
        РЕЗУЛЬТАТ
+--------------------+-------------+
|        Имя продукта|Имя категории|
+--------------------+-------------+
|        Nike Air 90s|         shoe|
|Nike Sportswear W...|        jaket|
|            Jordan 5|         shoe|
|       NonameProduct|         NULL|
+--------------------+-------------+


'''