import findspark
import pyspark
from pyspark.sql.types import IntegerType, FloatType, DateType, StringType
import pyspark.sql.functions as F


findspark.init()

spark_ui_port = 4040
app_name = "Otus_DZ_3"

spark = (
    pyspark.sql.SparkSession
        .builder
        .appName(app_name)
        .config("spark.driver.memory", "4g") 
        .config("spark.executor.memory", "4g")     # 4 ГБ для executor
        .config("spark.executor.cores", "2")       # 2 ядра на executor
        .config("spark.executor.instances", "6")   # 6 executor
        .getOrCreate()
)

# загрузка данных из файлов
data_text = spark.read.format("text").load("data/*.txt")

# Извлечение заголовков
header = data_text.filter(data_text.value.startswith("#")).first()[0]
columns = header[2:].split(" | ")

# Фильтрация данных
data = data_text.filter(~data_text.value.startswith("#"))

# Преобразование данных в DataFrame
df = data.selectExpr(
    f"split(value, ',')[0] as {columns[0]}",
    f"split(value, ',')[1] as {columns[1]}",
    f"split(value, ',')[2] as {columns[2]}",
    f"split(value, ',')[3] as {columns[3]}",
    f"split(value, ',')[4] as {columns[4]}",
    f"split(value, ',')[5] as {columns[5]}",
    f"split(value, ',')[6] as {columns[6]}",
    f"split(value, ',')[7] as {columns[7]}",
    f"split(value, ',')[8] as {columns[8]}"
)

# Преобразование типов данных
df = df.withColumn(columns[0], df[columns[0]].cast(IntegerType())) \
       .withColumn(columns[1], df[columns[1]].cast(DateType())) \
       .withColumn(columns[2], df[columns[2]].cast(IntegerType())) \
       .withColumn(columns[3], df[columns[3]].cast(IntegerType())) \
       .withColumn(columns[4], df[columns[4]].cast(FloatType())) \
       .withColumn(columns[5], df[columns[5]].cast(IntegerType())) \
       .withColumn(columns[6], df[columns[6]].cast(IntegerType())) \
       .withColumn(columns[7], df[columns[7]].cast(IntegerType())) \
       .withColumn(columns[8], df[columns[8]].cast(IntegerType())) 

# предобработа данных
df = df.withColumn("tx_amount", F.col("tx_amount").cast(FloatType()))
df = df.na.drop("any")
df = df.withColumnRenamed("tranaction_id", "transaction_id")
df = df.dropDuplicates(["transaction_id"])

# Сохранение данных в бакет
df.repartition(50).write.parquet("s3a://{{ s3_bucket }}/data_prepare/cleaned_fraud_dataset.parquet")

spark.stop()