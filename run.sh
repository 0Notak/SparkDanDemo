spark-submit \

# Ejemplo
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g \
  --conf spark.driver.cores=2 \
  --conf spark.driver.memory=4g \

  #Ejemplo
--files /path/to/tabla.json
--py-files /path/to/proy.py

