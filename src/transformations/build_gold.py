from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

def main():
    spark = get_spark_session('BuildGoldSales')
    orders = spark.read.parquet('data/silver/orders')
    items = spark.read.parquet('data/silver/order_items')
    customers = spark.read.parquet('data/silver/customers')
    products = spark.read.parquet('data/silver/products')
    fact = (items.join(orders,'order_id').join(customers,'customer_id').join(products,'product_id')
        .select('order_id','order_item_id','customer_id','product_id','order_timestamp','order_status','state','customer_segment','category','quantity',
                F.col('order_items.unit_price') if False else F.col('line_amount')))
    fact.write.mode('overwrite').partitionBy('state').parquet('data/gold/fact_sales')
    daily = (fact.withColumn('order_date', F.to_date('order_timestamp')).groupBy('order_date','state','category')
        .agg(F.round(F.sum('line_amount'),2).alias('revenue'), F.countDistinct('order_id').alias('orders'), F.sum('quantity').alias('units')))
    daily.write.mode('overwrite').parquet('data/gold/daily_sales')
    spark.stop()

if __name__ == '__main__': main()
