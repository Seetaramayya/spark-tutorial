#!/usr/bin/env bash

COUNT=$(docker ps | grep -c spark)

if [ $COUNT -gt 0 ]; then
  echo "Containers are up and running, so submitting the job"
else
  echo "Building the 'spark-tutorial' jar"
#  sbt clean assembly

  echo "Copying the jar to 'apps' directory"
  SCALA_VERSION=$(cat build.sbt | grep scalaVersion | cut -d '=' -f2 | sed 's/"//g' | cut -d '.' -f1,2)
  cp ~/target/"$SCALA_VERSION"/spark-tutorial.jar ./spark-cluster/apps/

  echo "Starting up the containers"
  docker-compose -f ./spark-cluster/docker-compose.yml up --detach

  sleep 1
  x=1
  ALIVE_COUNT=$(docker logs spark-master 2>&1 | grep -c ALIVE)
  while [ $ALIVE_COUNT -lt 0 ]; do
    x=$(( $x + 1 ))
    sleep 1
    echo "Waiting for spark-master to start"
    [[ $x -gt 9 ]] && echo "FAILED to start spark containers" && docker ps && exit 1
  done
fi

docker exec spark-master /spark/bin/spark-submit \
  --class learn.spark.deploy.TestDeploy \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --verbose --supervise \
  /opt/spark-apps/spark-tutorial.jar /opt/spark-data/movies.json /opt/spark-data/seeta-movies.json
