#!/bin/bash

ACTION=$1
BUCKET="tfg-metacritic-study"
JAR_PATH="../target/scala-2.12"
DATA_PATH="../src/main/scala/data"

if [ "$ACTION" == "up" ]; then
  echo "--- Creating Bucket S3 ---"
  aws s3api create-bucket --bucket $BUCKET --region us-east-1

  echo "--- Creating Folders Structure ---"
  aws s3api put-object --bucket $BUCKET --key jars/
  aws s3api put-object --bucket $BUCKET --key input/
  aws s3api put-object --bucket $BUCKET --key output/csv/
  aws s3api put-object --bucket $BUCKET --key output/parquet/
  aws s3api put-object --bucket $BUCKET --key logs/emr/
  aws s3api put-object --bucket $BUCKET --key logs/glue/
  aws s3api put-object --bucket $BUCKET --key scripts/

  echo "--- Uploading Datasets ---"
  aws s3 cp $DATA_PATH/metacritic_games_scores.csv s3://$BUCKET/input/
  aws s3 cp $DATA_PATH/metacritic_games_scores.parquet s3://$BUCKET/input/

  echo "--- Uploading JARs and Scripts ---"
  aws s3 cp $JAR_PATH/Metacritic-Videogame-Scores-Study-assembly-aws-csv.jar s3://$BUCKET/jars/
  aws s3 cp $JAR_PATH/Metacritic-Videogame-Scores-Study-assembly-aws-parquet.jar s3://$BUCKET/jars/
  aws s3 cp glue_script.scala s3://$BUCKET/scripts/

  echo "Bucket Ready"
elif [ "$ACTION" == "down" ]; then
  echo "--- Deleting Bucket S3 ---"
  aws s3 rm s3://$BUCKET --recursive
  aws s3 rb s3://$BUCKET

  echo "Bucket Deleted"
else
  echo "Use: ./aws_s3.sh [up|down]"
fi