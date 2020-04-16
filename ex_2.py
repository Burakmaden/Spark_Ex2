from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Example") \
    .getOrCreate()

categories = spark.read.csv("categories.csv", header=True)
categories.createOrReplaceTempView("CATEGORIES")

products = spark.read.csv("products.csv", header=True)
products.createOrReplaceTempView("PRODUCTS")

orders = spark.read.csv("orders.csv", header=True)


order_items = spark.read.csv("order_items.csv", header=True)
order_items.createOrReplaceTempView("ORDER_ITEMS")

orders.createOrReplaceTempView("ORDERS")

canceledOrders = spark.sql("SELECT * FROM ORDERS WHERE orderStatus = 'CANCELED' ")
canceledOrders.createOrReplaceTempView("ORDERS_C")

OrderList = spark.sql("SELECT *  FROM ORDERS_C INNER JOIN  ORDER_ITEMS ON ORDERS_C.orderId = ORDER_ITEMS.orderItemOrderId") 
OrderList = OrderList.withColumn("orderItemSubTotal", OrderList["orderItemSubTotal"].cast(DoubleType()))
OrderList = OrderList.orderBy(OrderList.orderItemSubTotal.desc())
OrderList.createOrReplaceTempView("ORDERLIST")

OrderList = spark.sql('''SELECT DISTINCT productCategoryId, categoryName, productName, orderItemSubTotal FROM (
    SELECT orderId, orderItemOrderId, orderItemSubTotal, productCategoryId, productCategoryId, productName 
    FROM ORDERLIST INNER JOIN PRODUCTS ON 
    ORDERLIST.orderItemProductId = PRODUCTS.productId) A 
    INNER JOIN  CATEGORIES ON A.productCategoryId = CATEGORIES.categoryId
    ''')
OrderList.show()

OrderList.write.mode('overwrite').parquet('canceledOrderList.parquet')