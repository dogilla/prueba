from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DoubleType, StringType

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Limpieza de datos") \
    .getOrCreate()

# Definir el esquema para ambos Dataframes que es el mismo
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True)
])

# Leer los DataFrames desde archivos CSV (asume que estan en la misma carpeta que el script)
df1 = spark.read.csv("cv1.csv", header=True, schema=schema)
df2 = spark.read.csv("cv2.csv", header=True,schema=schema)

# Union sencilla de los dataframes
df = df1.union(df2)

# Eliminar duplicados basados en la columna 'id' y lanza funcion de agregacion
cleaned_df = df.groupBy("id").agg(max("date").alias("date"))
#muestra los datos agregados
cleaned_df.show()
#resultado final
final_df = cleaned_df.join(df, ["id", "date"]).select("id", "date", "amount", "currency")

# Muestra el DataFrame con la consulta final
final_df.show()
