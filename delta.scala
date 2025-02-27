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

// Leer los datos de la tabla Delta
val deltaDF = spark.read.format("delta").load("/tmp/delta_table")
deltaDF.show()

// Crear referencia a la tabla Delta
val deltaTable = DeltaTable.forPath(spark, "/tmp/delta_table")

// 🔄 Actualizar datos usando MERGE (Upsert)
deltaTable.alias("target")
  .merge(
    spark.createDataFrame(Seq((2, "Bob Updated", 26))).toDF("id", "name", "age").alias("source"),
    "target.id = source.id"
  )
  .whenMatched().updateExpr(Map("name" -> "source.name", "age" -> "source.age"))
  .whenNotMatched().insertExpr(Map("id" -> "source.id", "name" -> "source.name", "age" -> "source.age"))
  .execute()

// Leer la tabla después del merge
spark.read.format("delta").load("/tmp/delta_table").show()

// 📜 Ver el historial de cambios en la tabla
deltaTable.history().show()

// ⏳ Consultar una versión anterior de la tabla
val oldVersionDF = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta_table")
oldVersionDF.show()

// 🧹 Ejecutar VACUUM para eliminar archivos obsoletos
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
deltaTable.vacuum(0.0)

// 📜 Ver historial después del VACUUM
deltaTable.history().show()

// 🗑️ Eliminar completamente la tabla Delta y su historial
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
fs.delete(new Path("/tmp/delta_table/"), true)  // Borra los datos y el historial
