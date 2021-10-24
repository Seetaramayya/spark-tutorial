# Test Deploy 

- Copy `spark-tutorial.jar` to `spark-cluster/apps`
- Bring the cluster up with the following command `docker-compose up --scale spark-worker=1`
- Connect to master node and run the following command

```shell
docker exec -it spark-master bash && /spark/bin/spark-submit \
  --class learn.spark.deploy.TestDeploy \
  --master spark://6ddc5d028936:7077 \
  --deploy-mode client \
  --verbose --supervise \
  /opt/spark-apps/spark-tutorial.jar /opt/spark-data/movies.json /opt/spark-data/goodMovies
```