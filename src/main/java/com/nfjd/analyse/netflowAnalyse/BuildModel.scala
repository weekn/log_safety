package com.nfjd.analyse.netflowAnalyse

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.PipelineModel

//com.nfjd.analyse.netflowAnalyse.BuildModel
object BuildModel {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("rm_build")//.setMaster("local[2]")
		conf.set("spark.yarn.executor.memoryOverhead",args(0))
		conf.set("es.nodes", "172.17.17.30").set("es.port", "9300")
		val cl=Array("spandtime", "packernum", "bytesize", "src_flow_port_num", "dst_flow_port_num", "tcp", "udp", "flownum")
		val cli=cl.map(c => s"${c}_imputed")
		
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//		val d = DataPretreatment.getData(spark)
		val d = spark.sqlContext.read.parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/train_df")
		//d.write.mode("overwrite").parquet("hdfs://172.17.17.24:8020/analyse/netflow_analyse/train_df")
val data_double = d.select(d.col("spandtime").cast(DoubleType).as("spandtime"),
d.col("packernum").cast(DoubleType).as("packernum"),
d.col("bytesize").cast(DoubleType).as("bytesize"),
d.col("src_flow_port_num").cast(DoubleType).as("src_flow_port_num"),
d.col("dst_flow_port_num").cast(DoubleType).as("dst_flow_port_num"),
d.col("tcp").cast(DoubleType).as("tcp"),
d.col("udp").cast(DoubleType).as("udp"),
d.col("flownum").cast(DoubleType).as("flownum"),
d.col("label")
)
		val data=data_double.repartition(600)
		val imputer = new Imputer().setInputCols(cl).setOutputCols(cli).setStrategy("mean").fit(data)
		
		
		val data2=imputer.transform(data)
//		//data.describe("spandtime", "packernum", "bytesize", "src_flow_port_num", "dst_flow_port_num", "tcp", "udp", "flownum").show()
		val dataAssemble = new VectorAssembler()
			.setInputCols(cli) //需要的feature
			.setOutputCol("features")
			// .transform(im_data)
		val data3=dataAssemble.transform(data2)
//		//dataWithFeatures.show()
//	
		val labelIndexer = new StringIndexer()
			.setInputCol("label")	
			.setOutputCol("indexedLabel")
			.fit(data3)
//
		val featureIndexer = new VectorIndexer()
			.setInputCol("features")
			.setOutputCol("indexedFeatures")
			.setMaxCategories(3)
			.fit(data3)
//
//		// Train a RandomForest model.
		val rf = new RandomForestClassifier()
			.setLabelCol("indexedLabel")
			.setFeaturesCol("indexedFeatures")
			.setMaxDepth(4)
			.setNumTrees(300)
			
//
//		// Convert indexed labels back to original labels.
		val labelConverter = new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)
//
//		// Chain indexers and forest in a Pipeline.
		val pipeline = new Pipeline()
			.setStages(Array(imputer,dataAssemble,labelIndexer, featureIndexer, rf,labelConverter))
//
//		// Train model. This also runs the indexers.
		val model = pipeline.fit(data)
		
		model.save("hdfs://172.17.17.24:8020/analyse/netflow_analyse/model")
//		val predictions=model.transform(dataWithFeatures)
//		predictions.show(100)
		
//val evaluator = new MulticlassClassificationEvaluator()
//.setLabelCol("indexedLabel")
//.setPredictionCol("prediction")
//.setMetricName("f1")

	}
}