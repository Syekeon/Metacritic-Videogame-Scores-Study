import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MetacriticStudyQueries extends App {
  val nThreads = '*'
  val dataFileFormat = "parquet" // "csv" o "parquet" para elegir el formato del dataset

  // Crear una SparkSession
  val spark = SparkSession.builder()
    .appName("Metacritic Study")
    .master(s"local[$nThreads]")
    .getOrCreate()

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

  val dataFilePath = dataFileFormat match {
    case "csv" =>
      "src/main/scala/data/metacritic_games_scores.csv"

    case "parquet" =>
      "src/main/scala/data/metacritic_games_scores.parquet"
  }

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

  /* CONSULTAS */

  // Comienzo del tiempo de ejecución de las consultas
  val startTimeExecution = System.nanoTime()

  val gamesFiltered = metacriticDataFrame
    .select("name", "developer", "publisher", "metascore", "user_score")
    .filter(col("metascore").isNotNull && col("user_score").isNotNull)
    .dropDuplicates("name")

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

  if (dataFileFormat == "csv") {
    scoresOverTime.write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/1_scores_over_time")
  } else {
    scoresOverTime.write.parquet("src/main/jupyter/data_filtered/parquet/1_scores_over_time")
  }

  // Consulta 2: Géneros mejor valorados
  val bestGenres = metacriticDataFrame
    .withColumn("genre", explode(split(col("genres"), ",")))
    .groupBy("genre")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") >= 20)

  if (dataFileFormat == "csv") {
    bestGenres.orderBy(desc("avg_metascore")).write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/2_best_genres_meta")
    bestGenres.orderBy(desc("avg_user_score")).write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/2_best_genres_user")
  } else {
    bestGenres.orderBy(desc("avg_metascore")).write.parquet("src/main/jupyter/data_filtered/parquet/2_best_genres_meta")
    bestGenres.orderBy(desc("avg_user_score")).write.parquet("src/main/jupyter/data_filtered/parquet/2_best_genres_user")
  }

  // Consulta 3: Desarrolladoras con mejor media de calidad
  val topDevs = gamesFiltered
    .withColumn("developer", explode(split(col("developer"), ",")))
    .groupBy("developer")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score"),
      count("*").alias("num_games")
    )
    .filter(col("num_games") >= 3 && col("developer").isNotNull)

  if (dataFileFormat == "csv") {
    topDevs.orderBy(desc("avg_metascore")).write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/3_top_developers_meta")
    topDevs.orderBy(desc("avg_user_score")).write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/3_top_developers_user")
  } else {
    topDevs.orderBy(desc("avg_metascore")).write.parquet("src/main/jupyter/data_filtered/parquet/3_top_developers_meta")
    topDevs.orderBy(desc("avg_user_score")).write.parquet("src/main/jupyter/data_filtered/parquet/3_top_developers_user")
  }

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

  if (dataFileFormat == "csv") {
    reviewsPerYear.write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/4_reviews_per_year")
  } else {
    reviewsPerYear.write.parquet("src/main/jupyter/data_filtered/parquet/4_reviews_per_year")
  }

  // Consulta 5: Géneros más infravalorados
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

  if (dataFileFormat == "csv") {
    underratedGenres.write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/5_underrated_genres")
  } else {
    underratedGenres.write.parquet("src/main/jupyter/data_filtered/parquet/5_underrated_genres")
  }

  // Consulta 6: Polarización de usuarios por videojuego
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

  if (dataFileFormat == "csv") {
    polarizingGames.write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/6_polarizing_games")
  } else {
    polarizingGames.write.parquet("src/main/jupyter/data_filtered/parquet/6_polarizing_games")
  }

  // Consulta 7: Discrepancia crítica vs usuario por publicadora
  val publisherDiscrepancy = gamesFiltered
    .groupBy("publisher")
    .agg(
      avg("metascore").alias("avg_metascore"),
      avg("user_score").alias("avg_user_score")
    )
    .filter(col("publisher").isNotNull && col("avg_metascore").isNotNull && col("avg_user_score").isNotNull)
    .withColumn("gap", abs(col("avg_metascore") - (col("avg_user_score") * 10)))
    .orderBy(desc("gap"))

  if (dataFileFormat == "csv") {
    publisherDiscrepancy.write.option("header", "true").csv("src/main/jupyter/data_filtered/csv/7_publisher_discrepancy")
  } else {
    publisherDiscrepancy.write.parquet("src/main/jupyter/data_filtered/parquet/7_publisher_discrepancy")
  }

  // Fin del tiempo de ejecución de las consultas
  val endTimeExecution = System.nanoTime()

  // Cálculo del tiempo de ejecución total
  val totalTimeExecution = (endTimeExecution - startTimeExecution) / 1e9d
  println(s"Queries execution time: $totalTimeExecution seconds")

  // Detener la SparkSession
  spark.stop()
}
