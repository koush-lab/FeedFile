from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('My App')\
        .enableHiveSupport()\
        .getOrCreate()

spark.conf.set("spark.dynamicAllocation.enabled","true")
spark.conf.set("spark.shuffle.service.enabled","true")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout","10s")
spark.conf.set("spark.dynamicAllocation.minExecutors","5")
spark.conf.set("spark.dynamicAllocation.maxExecutors","50")
spark.conf.set("spark.dynamicAllocation.initialExecutors","10")

spark.sparkContext.setLogLevel('WARN')
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.warn("My PySpark application is executing")

custom_schema = StructType([
StructField('Student_Name',StringType(),True),
StructField('Student_id',IntegerType(),True),
StructField('Student_Subj',StringType(),True),
StructField('month',StringType(),True),
StructField('year',StringType(),True),
StructField('Student_Subj',StringType(),True),
StructField('Student_Marks',DoubleType(),True)])

## Reading the file from the parameter table on Hive
feedfile=spark.sql("select param_value from tablename where <condition>")
df = spark.read.csv(fedfile,sep="~",schema=custom_schema,header="False")

## Adding Snapshot_period to the dataframe and writing it in append mode

df = df.withColumn("Snapshot_period", expr("case when period < 10 then year||'0'||month else year||month end"))
df.write.mode("append").format('hive').saveAsTable("hivedb.tablename")
spark.stop()
exit(0)