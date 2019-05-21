# Databricks notebook source
import numpy as np
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


# COMMAND ----------

dataset =  spark.read.format("csv").options(header='true',inforSchema = True, delimiter = ',').load("dbfs:/mnt/kp-adls/Automobile_price_data_Raw.csv")

# COMMAND ----------

dataset.printSchema()

# COMMAND ----------

dataset = dataset.select(col('make'),col('body-style'),col('wheel-base'),col('engine-size'),col('horsepower'),col('peak-rpm'),col('highway-mpg'),col('symboling'),col('price'))
dataset.drop('any')
dataset.describe().show()
dataset.count()

# COMMAND ----------

display(dataset)

# COMMAND ----------

#df = df.select(df.WellID,df.Field,DateConversion(df.Month).alias('Month'),df.AvgProduction.cast("double"),df.TotalProduction.cast("double"))
dataset = dataset.select(col('wheel-base').cast("float"),col('engine-size').cast("int"),col('horsepower').cast("int"),col('peak-rpm').cast("int"),col('highway-mpg').cast("int"),col('symboling').cast("int"),col('price').cast("int"))
# delete Nulls 
#data = dataset.dropna()
# Replace Column name whih has space
#dataset.count()

#exprs = [col(column).alias(column.replace(' ', '_')) for column in data.columns]
dataset_Features = VectorAssembler(inputCols=['wheel-base','engine-size','horsepower','peak-rpm','highway-mpg','symboling'],outputCol = "Features")
dataset_Features = dataset_Features.transform(data)

display(dataset_Features.select(col('horsepower')))

# COMMAND ----------

display(dataset_Features.select(col('Features')))

# COMMAND ----------

data_Final = dataset_Features.select(col('Features'),col('price'))
data_Final.show()

# COMMAND ----------

Data_Test,Data_Train = data_Final.randomSplit([0.75,0.25])

# COMMAND ----------

LinearRegressor = LinearRegression(featuresCol='Features',labelCol='price')
LinearRegressor = LinearRegressor.fit(Data_Train)

# COMMAND ----------

LinearRegressor.coefficients

# COMMAND ----------

Pred_Results = LinearRegressor.evaluate(Data_Test)

# COMMAND ----------

Pred_Results.predictions.show()

# COMMAND ----------


