#!/bin/bash

ACTION=$1
BUCKET="tfg-metacritic-study"
CSV_JOB_NAME="TFG-Metacritic-Study-CSV-Job"
PARQUET_JOB_NAME="TFG-Metacritic-Study-Parquet-Job"

if [ "$ACTION" == "up" ]; then
  echo "--- Creating Glue CSV Job ---"
  aws glue create-job \
    --name "$CSV_JOB_NAME" \
    --role LabRole \
    --glue-version "5.0" \
    --worker-type G.1X \
    --number-of-workers 2 \
    --command Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/glue_script.scala \
    --default-arguments '{
      "--class": "MetacriticStudyQueries",
      "--extra-jars": "s3://'$BUCKET'/jars/Metacritic-Videogame-Scores-Study-assembly-aws-csv.jar",
      "--enable-spark-ui": "true",
      "--spark-event-logs-path": "s3://'$BUCKET'/logs/glue/",
      "--job-language": "scala"
      }' \
    --timeout 30

  echo "--- Creating Glue Parquet Job ---"
  aws glue create-job \
    --name "$PARQUET_JOB_NAME" \
    --role LabRole \
    --glue-version "5.0" \
    --worker-type G.1X \
    --number-of-workers 2 \
    --command Name=glueetl,ScriptLocation=s3://$BUCKET/scripts/glue_script.scala \
    --default-arguments '{
      "--class": "MetacriticStudyQueries",
      "--extra-jars": "s3://'$BUCKET'/jars/Metacritic-Videogame-Scores-Study-assembly-aws-parquet.jar",
      "--enable-spark-ui": "true",
      "--spark-event-logs-path": "s3://'$BUCKET'/logs/glue/",
      "--job-language": "scala"
      }' \
    --timeout 30

  echo "--- Running Jobs Execution ---"
  aws glue start-job-run --job-name "$CSV_JOB_NAME"
  aws glue start-job-run --job-name "$PARQUET_JOB_NAME"

  echo "Jobs Executed"
elif [ "$ACTION" == "down" ]; then
  echo "--- Deleting Glue Jobs ---"
  aws glue delete-job --job-name "$CSV_JOB_NAME"
  aws glue delete-job --job-name "$PARQUET_JOB_NAME"

  echo "Jobs Deleted"
else
  echo "Use: ./aws_glue.sh [up|down]"
fi