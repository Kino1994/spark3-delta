import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import io.delta.tables._
import org.apache.hadoop.fs.{FileSystem, Path}

// Crear sesión de Spark con Delta Lake habilitado
val spark = SparkSession.builder()
  .appName("DeltaExample")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

// Definir esquema de la tabla
val schema = StructType(Seq(
  StructField("id", IntegerType, false),
  StructField("name", StringType, false),
  StructField("age", IntegerType, false)
))

// Crear datos de ejemplo
val data = Seq(
  Row(1, "Alice", 30),
  Row(2, "Bob", 25),
  Row(3, "Charlie", 40)
)

// Crear un DataFrame con los datos
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

// Guardar los datos en formato Delta
df.write.format("delta").mode("overwrite").save("/tmp/delta_table")

// 📡 Leer en streaming desde Delta Table y mostrar en consola
spark.readStream
  .format("delta")
  .load("/tmp/delta_table")
  .writeStream
  .format("console")
  .start()

// 🗑️ Opcionalmente, eliminar logs y datos de la tabla Delta
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
fs.delete(new Path("/tmp/delta_table/"), true)  // Elimina la tabla y el historial de transacciones

