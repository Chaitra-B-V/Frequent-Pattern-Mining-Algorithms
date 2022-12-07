// Databricks notebook source
// MAGIC %python
// MAGIC 
// MAGIC order_products__prior = spark.read.csv("/FileStore/tables/order_products__prior.csv", header=True, inferSchema=True)

// COMMAND ----------

// MAGIC %python
// MAGIC departments = spark.read.csv("/FileStore/tables/departments.csv", header=True, inferSchema=True)
// MAGIC aisles = spark.read.csv("/FileStore/tables/aisles.csv", header=True, inferSchema=True)
// MAGIC order_products_train = spark.read.csv("/FileStore/tables/order_products__train.csv", header=True, inferSchema=True)
// MAGIC orders = spark.read.csv("/FileStore/tables/orders.csv", header=True, inferSchema=True)
// MAGIC products = spark.read.csv("/FileStore/tables/products.csv", header=True, inferSchema=True)

// COMMAND ----------

// MAGIC %python
// MAGIC # Create Temporary Tables
// MAGIC aisles.createOrReplaceTempView("aisles")
// MAGIC departments.createOrReplaceTempView("departments")
// MAGIC order_products__prior.createOrReplaceTempView("order_products_prior")
// MAGIC order_products_train.createOrReplaceTempView("order_products_train")
// MAGIC orders.createOrReplaceTempView("orders")
// MAGIC products.createOrReplaceTempView("products")

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   count(order_id) as total_orders, 
// MAGIC   (case 
// MAGIC      when order_dow = '0' then 'Sunday'
// MAGIC      when order_dow = '1' then 'Monday'
// MAGIC      when order_dow = '2' then 'Tuesday'
// MAGIC      when order_dow = '3' then 'Wednesday'
// MAGIC      when order_dow = '4' then 'Thursday'
// MAGIC      when order_dow = '5' then 'Friday'
// MAGIC      when order_dow = '6' then 'Saturday'              
// MAGIC    end) as day_of_week 
// MAGIC   from orders  
// MAGIC  group by order_dow 
// MAGIC  order by total_orders desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   count(order_id) as total_orders, 
// MAGIC   order_hour_of_day as hour 
// MAGIC   from orders 
// MAGIC  group by order_hour_of_day 
// MAGIC  order by order_hour_of_day

// COMMAND ----------

// MAGIC %sql
// MAGIC select countbydept.*
// MAGIC   from (
// MAGIC   -- from product table, let's count number of records per dept
// MAGIC   -- and then sort it by count (highest to lowest) 
// MAGIC   select department_id, count(1) as counter
// MAGIC     from products
// MAGIC    group by department_id
// MAGIC    order by counter asc 
// MAGIC   ) as maxcount
// MAGIC inner join (
// MAGIC   -- let's repeat the exercise, but this time let's join
// MAGIC   -- products and departments tables to get a full list of dept and 
// MAGIC   -- prod count
// MAGIC   select
// MAGIC     d.department_id,
// MAGIC     d.department,
// MAGIC     count(1) as products
// MAGIC     from departments d
// MAGIC       inner join products p
// MAGIC          on p.department_id = d.department_id
// MAGIC    group by d.department_id, d.department 
// MAGIC    order by products desc
// MAGIC   ) countbydept 
// MAGIC   -- combine the two queries's results by matching the product count
// MAGIC   on countbydept.products = maxcount.counter

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(opp.order_id) as orders, p.product_name as popular_product
// MAGIC   from order_products_prior opp, products p
// MAGIC  where p.product_id = opp.product_id 
// MAGIC  group by popular_product 
// MAGIC  order by orders desc 
// MAGIC  limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select d.department, count(distinct p.product_id) as products
// MAGIC   from products p
// MAGIC     inner join departments d
// MAGIC       on d.department_id = p.department_id
// MAGIC  group by d.department
// MAGIC  order by products desc
// MAGIC  limit 10

// COMMAND ----------

// MAGIC %python
// MAGIC # Organize the data by shopping basket
// MAGIC from pyspark.sql.functions import collect_set, col, count
// MAGIC rawData = spark.sql("select p.product_name, o.order_id from products p inner join order_products_train o where o.product_id = p.product_id")
// MAGIC baskets = rawData.groupBy('order_id').agg(collect_set('product_name').alias('items'))
// MAGIC baskets.createOrReplaceTempView('baskets')
// MAGIC display(baskets)

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.ml.fpm.FPGrowth
// MAGIC 
// MAGIC // Extract out the items 
// MAGIC val baskets_ds = spark.sql("select items from baskets").as[Array[String]].toDF("items")
// MAGIC 
// MAGIC // Use FPGrowth
// MAGIC val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.001).setMinConfidence(0)
// MAGIC val model = fpgrowth.fit(baskets_ds)
// MAGIC import org.apache.spark.ml.fpm.FPGrowth

// COMMAND ----------

// MAGIC %scala
// MAGIC // Display frequent itemsets
// MAGIC val mostPopularItemInABasket = model.freqItemsets
// MAGIC mostPopularItemInABasket.createOrReplaceTempView("mostPopularItemInABasket")

// COMMAND ----------

// MAGIC %sql
// MAGIC select items, freq from mostPopularItemInABasket where size(items) > 2 order by freq desc limit 20

// COMMAND ----------

// MAGIC %scala
// MAGIC // Display generated association rules.
// MAGIC val ifThen = model.associationRules
// MAGIC ifThen.createOrReplaceTempView("ifThen")

// COMMAND ----------

// MAGIC %sql
// MAGIC select antecedent as `antecedent (if)`, consequent as `consequent (then)`, confidence from ifThen order by confidence desc limit 20
