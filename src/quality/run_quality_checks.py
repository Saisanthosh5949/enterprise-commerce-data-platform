from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

def assert_zero(label, value):
    if value != 0: raise ValueError(f'{label} failed: {value}')
    print(f'PASS: {label}')

def main():
    spark=get_spark_session('QualityChecks')
    c=spark.read.parquet('data/silver/customers'); o=spark.read.parquet('data/silver/orders'); i=spark.read.parquet('data/silver/order_items')
    assert_zero('null customer ids', c.filter(F.col('customer_id').isNull()).count())
    assert_zero('duplicate customer ids', c.groupBy('customer_id').count().filter('count > 1').count())
    assert_zero('null order ids', o.filter(F.col('order_id').isNull()).count())
    assert_zero('invalid item quantity', i.filter(F.col('quantity') <= 0).count())
    orphan=o.join(c,'customer_id','left_anti').count(); assert_zero('orphan orders', orphan)
    print('All data quality checks passed.')
    spark.stop()
if __name__=='__main__': main()
