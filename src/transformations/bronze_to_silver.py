from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

TABLES = ['customers','products','orders','order_items','payments','inventory']

def main():
    spark = get_spark_session('BronzeToSilver')
    spark.sparkContext.setLogLevel('WARN')
    for table in TABLES:
        df = spark.read.parquet(f'data/bronze/{table}')
        df = df.dropDuplicates()
        if table == 'customers': df = df.filter(F.col('customer_id').isNotNull() & F.col('email').contains('@'))
        elif table == 'products': df = df.filter(F.col('unit_price') > 0)
        elif table == 'orders': df = df.filter(F.col('order_id').isNotNull() & F.col('customer_id').isNotNull())
        elif table == 'order_items': df = df.filter((F.col('quantity') > 0) & (F.col('unit_price') > 0))
        df.write.mode('overwrite').parquet(f'data/silver/{table}')
        print(f'Silver complete: {table}')
    spark.stop()

if __name__ == '__main__': main()
