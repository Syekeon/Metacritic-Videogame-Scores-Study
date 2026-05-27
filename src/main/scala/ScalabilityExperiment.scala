import org.apache.spark.sql._

object ScalabilityExperiment extends App {
  val nThreads = "*"
  val dataFileFormat = "csv" // "csv" o "parquet" para elegir el formato del dataset
  val envExperiment = "local"   // "local" o "aws" para elegir el entorno de ejecución

  // Inicialización de la SparkSession (Nota: Si vas a ejecutar en AWS, debes quitar el ".master()")
  val spark = SparkSession.builder()
    .appName("TFG Metacritic Scalability Data Generator")
    .master(s"local[$nThreads]")
    .getOrCreate()

  // Factor multiplicador: Define cuántas veces se va a duplicar el dataset original
  val factor = 65

  // Asignación de las rutas de entrada y salida dependiendo del entorno seleccionado
  val (baseInputPath, baseOutputPath) = envExperiment match {
    case "local" => (
      "src/main/scala/data/metacritic_games_scores",
      s"src/main/scala/data/metacritic_games_scores_x${factor}"
    )

    case "aws" => (
      "s3://tfg-metacritic-study/input/metacritic_games_scores",
      s"s3://tfg-metacritic-study/input/metacritic_games_scores_x${factor}"
    )

    case _ => throw new IllegalArgumentException("Invalid environment. Use 'local' or 'aws'")
  }

  // Construcción de las rutas completas de entrada y salida basada en el formato elegido
  val inputPath = s"$baseInputPath.$dataFileFormat"
  val outputPath = s"$baseOutputPath.$dataFileFormat"

  // Lectura del dataset con el formato elegido
  val originalDF = dataFileFormat match {
    case "csv" =>
      spark.read.option("header", "true").csv(inputPath)

    case "parquet" =>
      spark.read.parquet(inputPath)

    case _ => throw new IllegalArgumentException("Invalid format. Use 'csv' or 'parquet'")
  }

  // Crea un DataFrame simple con una única columna llamada "id"
  val multiplier = spark.range(factor)

  // "crossJoin" hace un producto cartesiano: Cruza cada fila del dataset original con cada fila del DataFrame "multiplier"
  // "drop("id")" elimina la columna auxiliar creada por "range", dejando un DataFrame idéntico en estructura al original pero masivo
  val massiveDF = originalDF.crossJoin(multiplier).drop("id")

  // Escritura del nuevo dataset masivo con el formato elegido
  dataFileFormat match {
    case "csv" => massiveDF.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputPath)
    case "parquet" => massiveDF.coalesce(1).write.mode("overwrite").parquet(outputPath)
    case _ =>
  }

  // Detención de la SparkSession
  spark.stop()
}
