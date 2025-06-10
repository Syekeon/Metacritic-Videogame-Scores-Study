import org.apache.spark.sql._
import org.apache.spark.sql.types._

object CSVParquetTransform extends App {
  val spark = SparkSession.builder()
    .appName("CSV - Parquet Transform")
    .master("local[*]")
    .getOrCreate()

  val csvSchema = StructType(List(
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

  val csvFilePath = "src/main/scala/data/metacritic_games_scores.csv"

  val csvDataFrame = spark.read
    .schema(csvSchema)
    .option("header", "true")
    .csv(csvFilePath)

  csvDataFrame.coalesce(1).write.parquet("src/main/scala/data/metacritic_games_scores")
}
