import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MetacriticStudyQueries extends App {
  val nThreads = "*"
  val dataFileFormat = "csv" // "csv" o "parquet" para elegir el formato del dataset
  val envProject = "local"   // "local" o "aws" para elegir el entorno de ejecución

  // Asignación de las rutas de entrada y salida dependiendo del entorno seleccionado
  val (inputPath, outputPath) = envProject match {
    case "local" =>
      ("src/main/scala/data", "src/main/jupyter/data_filtered")

    case "aws" =>
      ("s3://tfg-metacritic-study/input", "s3://tfg-metacritic-study/output")

    case _ =>
      throw new IllegalArgumentException("Invalid environment. Use 'local' or 'aws'")
  }

  // Construcción de la ruta completa del conjunto de datos basada en el formato elegido
  val dataFilePath = dataFileFormat match {
    case "csv" =>
      s"$inputPath/metacritic_games_scores.csv"

    case "parquet" =>
      s"$inputPath/metacritic_games_scores.parquet"

    case _ =>
      throw new IllegalArgumentException("Invalid format. Use 'csv' or 'parquet'")
  }

  // Inicialización de la SparkSession (Nota: Si vas a ejecutar en AWS, debes quitar el ".master()")
  val spark = SparkSession.builder()
    .appName("TFG Metacritic Study")
    .master(s"local[$nThreads]")
    .getOrCreate()

  // Definición explícita del esquema de datos
  val dataSchema = StructType(List(
    StructField("link", StringType, true),
    StructField("name", StringType, true),
    StructField("developer", StringType, true),
    StructField("publisher", StringType, true),
    StructField("summary", StringType, true),
    StructField("genres", StringType, true),
    StructField("rating", StringType, true),
    StructField("platform", StringType, true),
    StructField("release_date", DateType, true),
    StructField("metascore", IntegerType, true),
    StructField("critic_reviews_count", IntegerType, true),
    StructField("positive_critic_reviews_count", IntegerType, true),
    StructField("mixed_critic_reviews_count", IntegerType, true),
    StructField("negative_critic_reviews_count", IntegerType, true),
    StructField("user_score", DoubleType, true),
    StructField("user_reviews_count", IntegerType, true),
    StructField("positive_user_reviews_count", IntegerType, true),
    StructField("mixed_user_reviews_count", IntegerType, true),
    StructField("negative_user_reviews_count", IntegerType, true)
  ))

  // Carga del DataFrame usando la ruta, esquema y opciones correspondientes al formato
  val metacriticDataFrame = dataFileFormat match {
    case "csv" =>
      spark.read
        .schema(dataSchema)
        .option("header", "true")
        .csv(dataFilePath)

    case "parquet" =>
      spark.read
        .schema(dataSchema)
        .parquet(dataFilePath)
  }

  // Función auxiliar para guardar los resultados de las queries
  def saveResults(df: DataFrame, folderName: String): Unit = {
    val fullOutputPath = s"$outputPath/$dataFileFormat/$folderName"

    if (dataFileFormat == "csv") {
      df.write.mode("overwrite").option("header", "true").csv(fullOutputPath)
    } else {
      df.write.mode("overwrite").parquet(fullOutputPath)
    }
  }

  /* QUERIES */

  // Comienzo del tiempo de ejecución de las queries
  val startTimeExecution = System.nanoTime()

  // Query auxiliar para filtrar videojuegos duplicados
  val gamesFiltered = metacriticDataFrame
    .select("name", "developer", "publisher", "metascore", "user_score")
    .filter(col("metascore").isNotNull && col("user_score").isNotNull)
    .dropDuplicates("name")

  // Query 1: Evolución de puntuaciones a lo largo del tiempo
  val scoresOverTime = metacriticDataFrame
    .withColumn("year", year(col("release_date")))
    .groupBy("year")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score")
    )
    .filter(col("year").isNotNull)
    .orderBy("year")

  saveResults(scoresOverTime, "1_scores_over_time")

  // Query 2: Géneros mejor valorados
  val bestGenres = metacriticDataFrame
    .withColumn("genre", explode(split(col("genres"), ",")))
    .groupBy("genre")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") >= 20)

  saveResults(bestGenres.orderBy(desc("avg_metascore")), "2_best_genres_meta")
  saveResults(bestGenres.orderBy(desc("avg_user_score")), "2_best_genres_user")

  // Query 3: Desarrolladoras con mejor media de calidad
  val topDevs = gamesFiltered
    .withColumn("developer", explode(split(col("developer"), ",")))
    .groupBy("developer")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") >= 3 && col("developer").isNotNull)

  saveResults(topDevs.orderBy(desc("avg_metascore")), "3_top_developers_meta")
  saveResults(topDevs.orderBy(desc("avg_user_score")), "3_top_developers_user")

  // Query 4: Reseñas por año de lanzamiento
  val reviewsPerYear = metacriticDataFrame
    .withColumn("year", year(col("release_date")))
    .groupBy("year")
    .agg(
      sum("user_reviews_count").alias("user_reviews"),
      sum("critic_reviews_count").alias("critic_reviews")
    )
    .filter(col("year").isNotNull)
    .orderBy(asc("year"))

  saveResults(reviewsPerYear, "4_reviews_per_year")

  // Query 5: Géneros más infravalorados
  val underratedGenres = metacriticDataFrame
    .withColumn("genre", explode(split(col("genres"), ",")))
    .groupBy("genre")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") >= 20)
    .withColumn("gap", (col("avg_user_score") * 10) - col("avg_metascore"))
    .orderBy(desc("gap"))

  saveResults(underratedGenres, "5_underrated_genres")

  // Query 6: Polarización de usuarios por videojuego
  val polarizingGames = metacriticDataFrame
    .withColumnRenamed("name", "game")
    .groupBy("game")
    .agg(
      sum("positive_user_reviews_count").alias("positive_user_reviews"),
      sum("negative_user_reviews_count").alias("negative_user_reviews"),
      sum("mixed_user_reviews_count").alias("mixed_user_reviews")
    )
    .withColumn("total_user_reviews", col("positive_user_reviews") + col("negative_user_reviews") + col("mixed_user_reviews"))
    .filter(col("total_user_reviews") >= 30)
    .withColumn("polarization", (col("positive_user_reviews") + col("negative_user_reviews")) / col("total_user_reviews"))
    .orderBy(desc("polarization"))

  saveResults(polarizingGames, "6_polarizing_games")

  // Query 7: Discrepancia crítica vs usuario por editora
  val publisherDiscrepancy = gamesFiltered
    .groupBy("publisher")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score")
    )
    .filter(col("publisher").isNotNull && col("avg_metascore").isNotNull && col("avg_user_score").isNotNull)
    .withColumn("gap", abs(col("avg_metascore") - (col("avg_user_score") * 10)))
    .orderBy(desc("gap"))

  saveResults(publisherDiscrepancy, "7_publisher_discrepancy")

  // Fin del tiempo de ejecución de las queries
  val endTimeExecution = System.nanoTime()

  // Cálculo del tiempo de ejecución total
  val totalTimeExecution = (endTimeExecution - startTimeExecution) / 1e9d
  println(s"Queries execution time: $totalTimeExecution seconds")

  // Detención de la SparkSession
  spark.stop()
}
