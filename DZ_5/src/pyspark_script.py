from argparse import ArgumentParser
import pyspark
from pyspark.sql.types import IntegerType, FloatType, DateType, StringType
import pyspark.sql.functions as F


def prepare_and_save_data(source_path: str, output_path: str) -> None:

    spark = (
        pyspark.sql.SparkSession
            .builder
            .appName("Otus_DZ_5")
            .config("spark.driver.memory", "4g") 
            .config("spark.executor.memory", "4g")     # 4 ГБ для executor
            .config("spark.executor.cores", "2")       # 2 ядра на executor
            .config("spark.executor.instances", "2")   # 3 executor
            .enableHiveSupport()
            .getOrCreate()
    )

    # загрузка данных из файлов
    data_text = spark.read.format("text").load(source_path)

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
    df.repartition(5).write.parquet(output_path)

    spark.stop()


def main():
    """Main function to execute the PySpark job"""
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set")

    input_path = f"s3a://{bucket_name}/input_data/*.txt"
    output_path = f"s3a://{bucket_name}/output_data/prepare_data.parquet"
    prepare_and_save_data(input_path, output_path)

if __name__ == "__main__":
    main()
