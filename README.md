# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch13

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch13_2.13-1.0.jar` (the same as name property in sbt file)


## 2, submit jar file, in project root dir
```
// common jar, need --jars option
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/scala-logging_2.13-3.9.4.jar \
  target/scala-2.13/main-scala-ch13_2.13-1.0.jar FLAT
```

## 3, print

### Case: FlattenShipmentDisplay
```
+--------------------+--------------------+----------+----------+--------------------+
|               books|            customer|      date|shipmentId|            supplier|
+--------------------+--------------------+----------+----------+--------------------+
|[{2, Spark with J...|{Chapel Hill, USA...|2019-10-05|    458922|{Shelter Island, ...|
+--------------------+--------------------+----------+----------+--------------------+

root
 |-- books: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- qty: long (nullable = true)
 |    |    |-- title: string (nullable = true)
 |-- customer: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- state: string (nullable = true)
 |-- date: string (nullable = true)
 |-- shipmentId: long (nullable = true)
 |-- supplier: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- state: string (nullable = true)


+--------------------+----------+----------+--------------------+--------------+----------------+--------------------+--------------+-------------+----------------+-------------------+--------------+
|               books|      date|shipmentId|               items| supplier_city|supplier_country|       supplier_name|supplier_state|customer_city|customer_country|      customer_name|customer_state|
+--------------------+----------+----------+--------------------+--------------+----------------+--------------------+--------------+-------------+----------------+-------------------+--------------+
|[{2, Spark with J...|2019-10-05|    458922|{2, Spark with Java}|Shelter Island|             USA|Manning Publications|      New York|  Chapel Hill|             USA|Jean Georges Perrin|North Carolina|
|[{2, Spark with J...|2019-10-05|    458922|{25, Spark in Act...|Shelter Island|             USA|Manning Publications|      New York|  Chapel Hill|             USA|Jean Georges Perrin|North Carolina|
|[{2, Spark with J...|2019-10-05|    458922|{1, Spark in Acti...|Shelter Island|             USA|Manning Publications|      New York|  Chapel Hill|             USA|Jean Georges Perrin|North Carolina|
+--------------------+----------+----------+--------------------+--------------+----------------+--------------------+--------------+-------------+----------------+-------------------+--------------+

root
 |-- books: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- qty: long (nullable = true)
 |    |    |-- title: string (nullable = true)
 |-- date: string (nullable = true)
 |-- shipmentId: long (nullable = true)
 |-- items: struct (nullable = true)
 |    |-- qty: long (nullable = true)
 |    |-- title: string (nullable = true)
 |-- supplier_city: string (nullable = true)
 |-- supplier_country: string (nullable = true)
 |-- supplier_name: string (nullable = true)
 |-- supplier_state: string (nullable = true)
 |-- customer_city: string (nullable = true)
 |-- customer_country: string (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- customer_state: string (nullable = true)


+----------+----------+--------------+----------------+--------------------+--------------+-------------+----------------+-------------------+--------------+---+--------------------+
|      date|shipmentId| supplier_city|supplier_country|       supplier_name|supplier_state|customer_city|customer_country|      customer_name|customer_state|qty|               title|
+----------+----------+--------------+----------------+--------------------+--------------+-------------+----------------+-------------------+--------------+---+--------------------+
|2019-10-05|    458922|Shelter Island|             USA|Manning Publications|      New York|  Chapel Hill|             USA|Jean Georges Perrin|North Carolina|  2|     Spark with Java|
|2019-10-05|    458922|Shelter Island|             USA|Manning Publications|      New York|  Chapel Hill|             USA|Jean Georges Perrin|North Carolina| 25|Spark in Action, ...|
|2019-10-05|    458922|Shelter Island|             USA|Manning Publications|      New York|  Chapel Hill|             USA|Jean Georges Perrin|North Carolina|  1|Spark in Action, ...|
+----------+----------+--------------+----------------+--------------------+--------------+-------------+----------------+-------------------+--------------+---+--------------------+

root
 |-- date: string (nullable = true)
 |-- shipmentId: long (nullable = true)
 |-- supplier_city: string (nullable = true)
 |-- supplier_country: string (nullable = true)
 |-- supplier_name: string (nullable = true)
 |-- supplier_state: string (nullable = true)
 |-- customer_city: string (nullable = true)
 |-- customer_country: string (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- customer_state: string (nullable = true)
 |-- qty: long (nullable = true)
 |-- title: string (nullable = true)

+----------+
|titleCount|
+----------+
|         3|
+----------+

root
 |-- titleCount: long (nullable = false)
```

### Case: RestaurantDocument
`df.groupBy(Columns).agg(collect_list())` remove columns not in gruopBy
```
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
|business_id|                name|            address|       city|state|postal_code|latitude|longitude|phone_number|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
| 4068010013|     BURGER KING 212| 600 JONES FERRY RD|   CARRBORO|   NC|      27510|    null|     null|+19199298395|
| 4068010016|CAROL WOODS CAFET...|750 WEAVER DAIRY RD|CHAPEL HILL|   NC|      27514|    null|     null|+19199183203|
| 4068010027|    COUNTRY JUNCTION|      402 WEAVER ST|   CARRBORO|   NC|      27510|    null|     null|+19199292462|
| 4068010037|          FARM HOUSE|  6004 MILLHOUSE RD|CHAPEL HILL|   NC|      27516|    null|     null|+19199295727|
| 4068010040|    FOUR-ELEVEN-WEST|  411 W FRANKLIN ST|CHAPEL HILL|   NC|      27514|    null|     null|+19199672782|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+
only showing top 5 rows
root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
-------> Get businessDF count: 301

+-----------+-----+--------+-------+
|business_id|score|    date|   type|
+-----------+-----+--------+-------+
| 4068010013| 95.5|20121029|routine|
| 4068010013| 92.5|20130606|routine|
| 4068010013|   97|20130920|routine|
| 4068010013|   98|20140609|routine|
| 4068010013| 97.5|20141125|routine|
+-----------+-----+--------+-------+
only showing top 5 rows
root
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)
-------> Get inspectionDF count: 5348

+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
|business_id|           name|           address|    city|state|postal_code|latitude|longitude|phone_number|business_id|score|    date|   type|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 95.5|20121029|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 92.5|20130606|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013|   97|20130920|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013|   98|20140609|routine|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 97.5|20141125|routine|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+
only showing top 5 rows
root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)
-------> Get joinDF count: 5317

+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+--------------------+
|business_id|           name|           address|    city|state|postal_code|latitude|longitude|phone_number|business_id|score|    date|   type|         temp_column|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+--------------------+
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 95.5|20121029|routine|{4068010013, 95.5...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 92.5|20130606|routine|{4068010013, 92.5...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013|   97|20130920|routine|{4068010013, 97, ...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013|   98|20140609|routine|{4068010013, 98, ...|
| 4068010013|BURGER KING 212|600 JONES FERRY RD|CARRBORO|   NC|      27510|    null|     null|+19199298395| 4068010013| 97.5|20141125|routine|{4068010013, 97.5...|
+-----------+---------------+------------------+--------+-----+-----------+--------+---------+------------+-----------+-----+--------+-------+--------------------+
only showing top 5 rows
root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- business_id: string (nullable = true)
 |-- score: string (nullable = true)
 |-- date: string (nullable = true)
 |-- type: string (nullable = true)
 |-- temp_column: struct (nullable = false)
 |    |-- business_id: string (nullable = true)
 |    |-- score: string (nullable = true)
 |    |-- date: string (nullable = true)
 |    |-- type: string (nullable = true)
-------> Get tempDF count: 5317

+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
|business_id|                name|            address|       city|state|postal_code|latitude|longitude|phone_number|         inspections|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
| 4068010013|     BURGER KING 212| 600 JONES FERRY RD|   CARRBORO|   NC|      27510|    null|     null|+19199298395|[{4068010013, 95....|
| 4068010016|CAROL WOODS CAFET...|750 WEAVER DAIRY RD|CHAPEL HILL|   NC|      27514|    null|     null|+19199183203|[{4068010016, 94....|
| 4068010027|    COUNTRY JUNCTION|      402 WEAVER ST|   CARRBORO|   NC|      27510|    null|     null|+19199292462|[{4068010027, 97....|
| 4068010037|          FARM HOUSE|  6004 MILLHOUSE RD|CHAPEL HILL|   NC|      27516|    null|     null|+19199295727|[{4068010037, 97....|
| 4068010040|    FOUR-ELEVEN-WEST|  411 W FRANKLIN ST|CHAPEL HILL|   NC|      27514|    null|     null|+19199672782|[{4068010040, 99,...|
+-----------+--------------------+-------------------+-----------+-----+-----------+--------+---------+------------+--------------------+
only showing top 5 rows
root
 |-- business_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- inspections: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- business_id: string (nullable = true)
 |    |    |-- score: string (nullable = true)
 |    |    |-- date: string (nullable = true)
 |    |    |-- type: string (nullable = true)
-------> Get nestedDF count: 301
```


## 4, Some diffcult case

### How to pass Seq to var-args functions
```
def foo(os: String*) = os.toList.foreach(println)

val args = Seq("hi", "there")

foo(args:_*)
```