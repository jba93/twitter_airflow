from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    df = spark.read.json(
        "/home/juliana/Área de Trabalho/datapipeline/airflow/datalake/twitter_aluraonline/extract_date=2022-11-17/AluraOnline_20221117.json"
    )