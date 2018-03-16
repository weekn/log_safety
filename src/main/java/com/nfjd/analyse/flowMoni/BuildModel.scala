package com.nfjd.analyse.flowMoni

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.AtomicType
import java.net.URLDecoder
import org.apache.spark.ml.linalg.DenseVector
import breeze.linalg.norm
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame

//com.nfjd.analyse.flowMoni.BuildModel

object BuildModel {

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("MoniFlowAnalyse_train") //.setMaster("local[2]")
		conf.set("es.nodes", "172.17.17.30,172.17.17.31,172.17.17.32").set("es.port", "9300")
		conf.set("spark.yarn.executor.memoryOverhead", "4024")
		//conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用Kryo序列化
		//conf.set("spark.kryo.registrator", "com.nfjd.analyse.flowMoni.MyRegisterKryo")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val datapretreater = new DataPretreater(sqlContext)
		val traindata = datapretreater.getTrainData()
		
		val regexTokenizer = new RegexTokenizer()
			.setInputCol("uri")
			.setOutputCol("words")
			.setPattern("""\s|\(|\)|\-|\d|\,|\;|\/|\?|\%|\||\.""")
		val data1 = regexTokenizer.transform(traindata)

		val word2Vec = new Word2Vec()
			.setInputCol("words")
			.setOutputCol("words2vec")
			.setVectorSize(300).setMaxIter(300)
			.setMinCount(1).fit(data1)
		val data2 = word2Vec.transform(data1)
		val labelIndexer = new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(data2)
		// Automatically identify categorical features, and index them.
		val featureIndexer = new VectorIndexer()
			.setInputCol("words2vec")
			.setOutputCol("indexedFeatures")
			.setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
			.fit(data2)

		

		val rf = new RandomForestClassifier()
			.setLabelCol("indexedLabel")
			.setFeaturesCol("indexedFeatures")
			.setMaxDepth(4)
			.setSubsamplingRate(0.8)
			.setNumTrees(300)
			.setPredictionCol("predictionRF").setProbabilityCol("probabilityRF").setRawPredictionCol("rawprobabilityRF")

		val lr = new LogisticRegression()
			.setMaxIter(200)
			.setRegParam(0.3)
			.setElasticNetParam(0.8)
			.setLabelCol("indexedLabel")
			.setFeaturesCol("indexedFeatures")
			.setPredictionCol("predictionLR").setProbabilityCol("probabilityLR").setRawPredictionCol("rawprobabilityLR")

		// Convert indexed labels back to original labels.
		val labelConverterLR = new IndexToString()
			.setInputCol("predictionLR")
			.setOutputCol("predictedLabelLR")
			.setLabels(labelIndexer.labels)
		val labelConverterRF = new IndexToString()
			.setInputCol("predictionRF")
			.setOutputCol("predictedLabelRF")
			.setLabels(labelIndexer.labels)
		val pipeline = new Pipeline()
			.setStages(Array(regexTokenizer, word2Vec, labelIndexer, featureIndexer, lr, rf, labelConverterLR, labelConverterRF))
		val model = pipeline.fit(traindata)

		model.save("hdfs://172.17.17.24:8020/analyse/flowMoni/model")
		//		val predictions = model.transform(data_test)
		//
		//		val evaluator = new MulticlassClassificationEvaluator()
		//			.setLabelCol("indexedLabel")
		//			.setPredictionCol("prediction")
		//			.setMetricName("accuracy")
		//		val accuracy = evaluator.evaluate(predictions)
		//		println("Test Error = " + (1.0 - accuracy))

	}

}