/opt/bitnami/spark/bin/spark-submit \
  --class it.unisa.hpc.spark.worldcupstats.WorldCupStats \
  --master local \
  ./SparkWorldCupStats.jar \
  ./input 10 3