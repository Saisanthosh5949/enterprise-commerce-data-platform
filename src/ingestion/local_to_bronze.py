from src.common.spark_session import get_spark_session
TABLES=['customers','products','orders','order_items','payments','inventory']
def main():
    spark=get_spark_session('LocalToBronze')
    for t in TABLES:
        df=spark.read.parquet(f'data/raw/{t}')
        df.write.mode('overwrite').parquet(f'data/bronze/{t}')
        print(f'Bronze ingested: {t}')
    spark.stop()
if __name__=='__main__': main()
