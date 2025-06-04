import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MetacriticStudyQueries extends App {
  val nThreads = '*'

  // Crear una SparkSession
  val spark = SparkSession.builder()
    .appName("Metacritic Study")
    .master(s"local[$nThreads]")
    .getOrCreate()

  // Ruta al archivo CSV
  val dataFilePath = "src/main/scala/data/metacritic_games_scores.csv"

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

  // Leer el archivo CSV
  val metacriticDataFrame = spark.read
    .schema(dataSchema)
    .option("header", "true")
    .csv(dataFilePath)


  /* CONSULTAS */

  // Comienzo del tiempo de ejecución de las consultas
  val startTimeExecution = System.nanoTime()

  // Consulta 1: Evolución de puntuaciones a lo largo del tiempo
  val scoresOverTime = metacriticDataFrame
    .withColumn("year", year(col("release_date")))
    .groupBy("year")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score")
    )
    .filter(col("year").isNotNull)
    .orderBy("year")

  scoresOverTime.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/1_scores_over_time")

  // Consulta 2: Géneros mejor valorados
  val bestGenres = metacriticDataFrame
    .withColumn("genre", explode(split(col("genres"), ",")))
    .groupBy("genre")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") > 10)
    .orderBy(desc("avg_metascore"))

  bestGenres.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/2_best_genres")

  // Consulta 3: Desarrolladoras con mejor media de calidad
  val topDevs = metacriticDataFrame
    .groupBy("developer")
    .agg(
      avg("metascore").alias("avg_metascore"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") >= 3 && col("developer").isNotNull)
    .orderBy(desc("avg_metascore"))

  topDevs.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/3_top_developers")

  // Consulta 4: Reseñas por año de lanzamiento
  val reviewsPerYear = metacriticDataFrame
    .withColumn("year", year(col("release_date")))
    .groupBy("year")
    .agg(
      sum("user_reviews_count").alias("user_reviews"),
      sum("critic_reviews_count").alias("critic_reviews")
    )
    .filter(col("year").isNotNull)
    .orderBy(asc("year"))

  reviewsPerYear.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/4_reviews_per_year")

  // Consulta 5: Géneros más infravalorados
  val underratedGenres = metacriticDataFrame
    .withColumn("genre", explode(split(col("genres"), ",")))
    .groupBy("genre")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") > 10)
    .withColumn("gap", (col("avg_user_score") * 10) - col("avg_metascore"))
    .orderBy(desc("gap"))

  underratedGenres.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/5_underrated_genres")

  // Consulta 6: Polarización de usuarios por juego
  val polarizingGames = metacriticDataFrame
    .withColumn("polarization", (col("positive_user_reviews_count") + col("negative_user_reviews_count")) / col("user_reviews_count"))
    .filter(col("user_reviews_count") > 10 && col("polarization").isNotNull)
    .select("name", "polarization", "positive_user_reviews_count", "negative_user_reviews_count")
    .orderBy(desc("polarization"))

  polarizingGames.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/6_polarizing_games")

  // Consulta 7: Discrepancia crítica vs usuario por publicadora
  val publisherDiscrepancy = metacriticDataFrame
    .groupBy("publisher")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score")
    )
    .filter(col("publisher").isNotNull && col("avg_metascore").isNotNull && col("avg_user_score").isNotNull)
    .withColumn("gap", abs(col("avg_metascore") - (col("avg_user_score") * 10)))
    .orderBy(desc("gap"))

  publisherDiscrepancy.coalesce(1).write.option("header", "true").csv("src/main/jupyter/data_filtered/7_publisher_discrepancy")

  // Fin del tiempo de ejecución de las consultas
  val endTimeExecution = System.nanoTime()

  // Cálculo del tiempo de ejecución total
  val totalTimeExecution = (endTimeExecution - startTimeExecution) / 1e9d
  println(s"Queries execution time: $totalTimeExecution seconds")

  // Detener la SparkSession
  spark.stop()
}
