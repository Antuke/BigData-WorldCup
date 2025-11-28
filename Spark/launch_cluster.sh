/opt/bitnami/spark/bin/spark-submit \
  --class it.unisa.hpc.spark.worldcupstats.WorldCupStats \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --supervise \
  --executor-memory 1G \
 ./SparkWorldCupStats.jar \
./input 15 3
