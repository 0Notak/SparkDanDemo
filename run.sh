archivo_python=$1  
argumento=$2

spark-submit $archivo_python $argumento \

# Ejemplo
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.executor.cores=8 \
  --conf spark.executor.memory=32g \
  --conf spark.driver.cores=32 \
  --conf spark.driver.memory=128g \

  #Ejemplo
--files /path/to/tabla.json
--py-files /path/to/proy.py

