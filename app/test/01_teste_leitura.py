#%pyspark
from pyspark.sql import SparkSession
spark.stop()
spark = SparkSession.builder \
    .appName("MinIO-Access-Fix") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "senhasegura") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()
    
df = spark.read.parquet("s3a://raw-bucket/teste/")
df.show()

"""
+--------------+--------------------+--------------------+--------------------+----+--------------+-----------+--------------------+-------+
|account_number|           bank_name|dt_current_timestamp|                iban|  id|routing_number|  swift_bic|                 uid|user_id|
+--------------+--------------------+--------------------+--------------------+----+--------------+-----------+--------------------+-------+
|    9015971456|ABN AMRO HOARE GO...|       1619454557330|GB21HCRO995723481...| 102|     103784249|BOFAGB4TLTR|8bbe2f2f-0b7b-4a7...|   8823|
|    7559729504|ABU DHABI ISLAMIC...|       1619454557330|GB47NWEK897640914...|7663|     329902119|   AACCGB21|a7a1851f-a157-443...|   1284|
|    9579663831|UBS CLEARING AND ...|       1619454557330|GB12ALHQ962488106...|2147|     625893346|   AAPUGB21|09e2402e-5c1c-488...|   9552|
|    0578673720|ABN AMRO HOARE GO...|       1619454557330|GB38NBTL207975050...|8583|     286940997|BCYPGB2LMBB|82fe6409-181c-469...|   8799|
|    6708887811|ALKEN ASSET MANAG...|       1619454557330|GB54XHIZ270346356...|7228|     679071295|BCYPGB2LHHB|8bed0a03-c369-414...|   3454|
+--------------+--------------------+--------------------+--------------------+----+--------------+-----------+--------------------+-------+
"""

#%pyspark
df = spark.read.option('multiline','true').json("s3a://raw-bucket/json/")
df.show()
"""
+---+---------------+--------------+-----------+--------------------+--------------------------+-------------------+-----------+----------+------+-----+-----------------------+---------+-------------+----------------------+-------------------+--------------+-----------+---------------------+-------------------+----------+--------------------+-----------+
|Age|Assignments_Avg|Attendance (%)| Department|               Email|Extracurricular_Activities|Family_Income_Level|Final_Score|First_Name|Gender|Grade|Internet_Access_at_Home|Last_Name|Midterm_Score|Parent_Education_Level|Participation_Score|Projects_Score|Quizzes_Avg|Sleep_Hours_per_Night|Stress_Level (1-10)|Student_ID|Study_Hours_per_Week|Total_Score|
+---+---------------+--------------+-----------+--------------------+--------------------------+-------------------+-----------+----------+------+-----+-----------------------+---------+-------------+----------------------+-------------------+--------------+-----------+---------------------+-------------------+----------+--------------------+-----------+
| 22|          84.22|         52.29|Engineering|student0@universi...|                        No|             Medium|      57.82|      Omar|Female|    F|                    Yes| Williams|        55.03|           High School|               3.99|          85.9|      74.06|                  4.7|                  5|     S1000|                 6.2|      56.09|
| 18|           null|         97.27|Engineering|student1@universi...|                        No|             Medium|       45.8|     Maria|  Male|    A|                    Yes|    Brown|        97.23|                  None|               8.32|         55.65|      94.24|                  9.0|                  4|     S1001|                19.0|      50.64|
| 24|           67.7|         57.19|   Business|student2@universi...|                        No|                Low|      93.68|     Ahmed|  Male|    D|                    Yes|    Jones|        67.05|              Master's|               5.05|         73.79|       85.7|                  6.2|                  6|     S1002|                20.7|       70.3|
| 24|          66.06|         95.15|Mathematics|student3@universi...|                       Yes|               High|      80.63|      Omar|Female|    A|                    Yes| Williams|        47.79|           High School|               6.54|         92.12|      93.51|                  6.7|                  3|     S1003|                24.8|      61.63|
| 23|          96.85|         54.18|         CS|student4@universi...|                       Yes|               High|      78.89|      John|Female|    F|                    Yes|    Smith|        46.59|           High School|               5.97|         68.42|       83.7|                  7.1|                  2|     S1004|                15.4|      66.13|
+---+---------------+--------------+-----------+--------------------+--------------------------+-------------------+-----------+----------+------+-----+-----------------------+---------+-------------+----------------------+-------------------+--------------+-----------+---------------------+-------------------+----------+--------------------+-----------+
"""

#%pyspark
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/teste") \
    .option("user", "admin") \
    .option("password", "senhasegura") \
    .option("dbtable", "songs") \
    .load()

df.show(5)

"""
+-----------+-----------------+---------+-------------+----------+------------+-------------+--------------+----------------+---------------+------------+------------+------+
|       Song|           Artist|  Streams|Daily Streams|     Genre|Release Year|Peak Position|Weeks on Chart|Lyrics Sentiment|TikTok Virality|Danceability|Acousticness|Energy|
+-----------+-----------------+---------+-------------+----------+------------+-------------+--------------+----------------+---------------+------------+------------+------+
|Track 14728|         EchoSync|689815326|       796199|      Trap|        2021|           81|             8|             0.2|             17|        0.11|        0.59|   0.6|
|Track 21319|The Midnight Howl|457954557|      2426710|Electronic|        2018|           44|            99|            0.51|             30|        0.61|        0.25|  0.71|
|Track 22152|  Retro Resonance|217316865|      1639915|    Reggae|        1992|           57|            12|            0.36|             11|        0.43|        0.58|   0.2|
|Track 80217|   Urban Rhapsody|312747634|      3614532|       Pop|        2000|           21|            50|            0.89|             44|        0.18|        0.04|  0.63|
|Track 77204|     Sofia Carter|726442597|      1028518|     Blues|        2001|           97|             9|           -0.62|             71|        0.82|        0.59|  0.61|
+-----------+-----------------+---------+-------------+----------+------------+-------------+--------------+----------------+---------------+------------+------------+------+
"""