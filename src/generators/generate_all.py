from pyspark.sql import functions as F
from src.common.spark_session import get_spark_session

CUSTOMERS = 100_000
PRODUCTS = 20_000
ORDERS = 1_000_000


def write(df, name):
    path = f'data/raw/{name}'
    df.write.mode('overwrite').parquet(path)
    print(f'Wrote {name} -> {path}')


def main():
    spark = get_spark_session('GenerateEcommerceData')
    spark.sparkContext.setLogLevel('WARN')

    customers = (spark.range(1, CUSTOMERS + 1).withColumnRenamed('id','customer_number')
        .withColumn('customer_id', F.concat(F.lit('C'), F.lpad(F.col('customer_number'), 8, '0')))
        .withColumn('email', F.concat(F.lit('customer'), F.col('customer_number'), F.lit('@example.com')))
        .withColumn('state', F.element_at(F.array(*[F.lit(x) for x in ['TX','CA','CO','NY','FL','WA','IL','AZ']]), (F.floor(F.rand(42)*8)+1).cast('int')))
        .withColumn('customer_segment', F.element_at(F.array(*[F.lit(x) for x in ['Consumer','Corporate','Small Business']]), (F.floor(F.rand(43)*3)+1).cast('int')))
        .withColumn('created_at', F.expr('current_timestamp() - INTERVAL 1 DAY * CAST(rand(44) * 1095 AS INT)'))
        .select('customer_id','customer_number','email','state','customer_segment','created_at'))
    write(customers, 'customers')

    products = (spark.range(1, PRODUCTS + 1).withColumnRenamed('id','product_number')
        .withColumn('product_id', F.concat(F.lit('P'), F.lpad(F.col('product_number'), 7, '0')))
        .withColumn('product_name', F.concat(F.lit('Product '), F.col('product_number')))
        .withColumn('category', F.element_at(F.array(*[F.lit(x) for x in ['Electronics','Home','Beauty','Sports','Office','Clothing']]), (F.floor(F.rand(45)*6)+1).cast('int')))
        .withColumn('unit_price', F.round(F.rand(46)*495 + 5, 2))
        .select('product_id','product_number','product_name','category','unit_price'))
    write(products, 'products')

    orders = (spark.range(1, ORDERS + 1).withColumnRenamed('id','order_number')
        .withColumn('order_id', F.concat(F.lit('O'), F.lpad(F.col('order_number'), 10, '0')))
        .withColumn('customer_number', (F.floor(F.rand(47)*CUSTOMERS)+1).cast('long'))
        .withColumn('customer_id', F.concat(F.lit('C'), F.lpad(F.col('customer_number'), 8, '0')))
        .withColumn('order_status', F.element_at(F.array(*[F.lit(x) for x in ['COMPLETED','SHIPPED','PROCESSING','CANCELLED','REFUNDED']]), (F.floor(F.rand(48)*5)+1).cast('int')))
        .withColumn('order_timestamp', F.expr('current_timestamp() - INTERVAL 1 DAY * CAST(rand(49) * 730 AS INT)'))
        .select('order_id','order_number','customer_id','order_status','order_timestamp'))
    write(orders, 'orders')

    items = (spark.range(1, ORDERS*3 + 1).withColumnRenamed('id','order_item_id')
        .withColumn('order_number', (((F.col('order_item_id')-1)/3).cast('long')+1))
        .withColumn('order_id', F.concat(F.lit('O'), F.lpad(F.col('order_number'), 10, '0')))
        .withColumn('product_number', (F.floor(F.rand(50)*PRODUCTS)+1).cast('long'))
        .withColumn('product_id', F.concat(F.lit('P'), F.lpad(F.col('product_number'), 7, '0')))
        .withColumn('quantity', (F.floor(F.rand(51)*5)+1).cast('int'))
        .withColumn('unit_price', F.round(F.rand(52)*495+5, 2))
        .withColumn('line_amount', F.round(F.col('quantity')*F.col('unit_price'),2))
        .select('order_item_id','order_id','product_id','quantity','unit_price','line_amount'))
    write(items, 'order_items')

    payments = (orders.select('order_id','order_number')
        .withColumn('payment_id', F.concat(F.lit('PAY'), F.lpad(F.col('order_number'),10,'0')))
        .withColumn('payment_method', F.element_at(F.array(*[F.lit(x) for x in ['CARD','PAYPAL','APPLE_PAY','BANK']]), (F.floor(F.rand(53)*4)+1).cast('int')))
        .withColumn('payment_status', F.element_at(F.array(*[F.lit(x) for x in ['SUCCESS','SUCCESS','SUCCESS','FAILED','REFUNDED']]), (F.floor(F.rand(54)*5)+1).cast('int')))
        .select('payment_id','order_id','payment_method','payment_status'))
    write(payments, 'payments')

    inventory = (products.select('product_id')
        .withColumn('warehouse_id', F.element_at(F.array(F.lit('WH-TX'),F.lit('WH-CO'),F.lit('WH-CA')), (F.floor(F.rand(55)*3)+1).cast('int')))
        .withColumn('quantity_on_hand', (F.floor(F.rand(56)*1000)).cast('int'))
        .withColumn('updated_at', F.current_timestamp()))
    write(inventory, 'inventory')
    spark.stop()

if __name__ == '__main__': main()
