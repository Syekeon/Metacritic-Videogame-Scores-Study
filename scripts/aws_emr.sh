#!/bin/bash

ACTION=$1
CLUSTER_ID=$2
BUCKET="tfg-metacritic-study"

if [ "$ACTION" == "up" ]; then
  echo "--- Creating EMR Cluster ---"
  CLUSTER_ID=$(aws emr create-cluster \
    --name "TFG-Metacritic-Study-Cluster" \
    --release-label emr-7.3.0 \
    --applications Name=Spark Name=Hadoop \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
    --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
    --log-uri s3://$BUCKET/logs/emr \
    --query 'ClusterId' --output text)

  echo "--- Adding Steps to Cluster ---"
  aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --steps Type=Spark,Name="CSV Run",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--class,MetacriticStudyQueries,s3://$BUCKET/jars/Metacritic-Videogame-Scores-Study-assembly-aws-csv.jar]

  aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --steps Type=Spark,Name="Parquet Run",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--class,MetacriticStudyQueries,s3://$BUCKET/jars/Metacritic-Videogame-Scores-Study-assembly-aws-parquet.jar]

  echo "Cluster Created (ID: $CLUSTER_ID) and Steps Added"
elif [ "$ACTION" == "down" ]; then
  if [ -z "$CLUSTER_ID" ]; then
    echo "--- Searching Active Cluster... ---"
    CLUSTER_ID=$(aws emr list-clusters --active --query 'Clusters[0].Id' --output text)
  fi

  if [ "$CLUSTER_ID" != "None" ]; then
    echo "--- Terminating Cluster ---"
    aws emr terminate-clusters --cluster-ids "$CLUSTER_ID"

    echo "Cluster Terminated"
  else
    echo "Active Cluster Not Found"
  fi
else
  echo "Use: ./aws_emr.sh up"
  echo "Use: ./aws_emr.sh down [cluster-id]"
fi