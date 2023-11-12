package nl.inholland.classification;

import org.apache.spark.ml.feature.*;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.sql.*;

public class Classification {
    // Set constants
    private static final String INPUT_FILE_PATH = "data/Fraud.csv";
    private static final Integer LIMIT = 100000; // Caution: setting this too high can cause memory issues!

    // Create variables
    private static Dataset<Row> data;
    private static Dataset<Row> trainingData;
    private static Dataset<Row> testData;

    public static void main(String[] args) {
        // Create Spark session
        SparkContext sparkContext = new SparkContext("local[*]", "FraudDetectionApp");
        sparkContext.setLogLevel("ERROR");
        SparkSession spark = SparkSession.builder().getOrCreate();
        // Read data into a DataFrame
        data = spark.read().option("header", "true").csv(INPUT_FILE_PATH);
        
        // PREPROCESSING
        preprocessing();

        // TRAINING
        Dataset<Row> predictions = training();

        // EVALUATION
        double accuracy = evaluation(predictions);
        // Print the accuracy of the model
        System.out.println("Test set accuracy = " + accuracy);

        // Stop the Spark session
        spark.stop();
    }

    private static void preprocessing(){
        // Specify the schema of the data
        data = data.withColumn("amount", data.col("amount").cast("integer"));
        data = data.withColumn("oldbalanceOrg", data.col("oldbalanceOrg").cast("integer"));
        data = data.withColumn("newbalanceOrig", data.col("newbalanceOrig").cast("integer"));
        data = data.withColumn("oldbalanceDest", data.col("oldbalanceDest").cast("integer"));
        data = data.withColumn("newbalanceDest", data.col("newbalanceDest").cast("integer"));
        data = data.withColumn("isFraud", data.col("isFraud").cast("numeric"));

        // Drop rows with missing values
        data = data.na().drop();

        // Limit the amount of data
        data = data.limit(LIMIT);

        // Index the categorical columns
        StringIndexer indexer = new StringIndexer()
                .setInputCols(new String[] { "type", })
                .setOutputCols(new String[] { "typeIndex", });

        // Fit the indexer and transform the data
        data = indexer.fit(data).transform(data);

        // Tokenize the input text
        // This splits the text into a list of words
        Tokenizer tokenizer = new Tokenizer().setInputCol("nameOrig").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(data);

        // Apply TF (Term Frequency) on the words
        // This converts the words into a numerical representation
        HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000);
        Dataset<Row> featurizedData = hashingTF.transform(wordsData);
        
        // Apply IDF (Inverse Document Frequency) on the features
        // This downscales the words that occur frequently in the documents
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        featurizedData = idf.fit(featurizedData).transform(featurizedData);

        // Combine features into a single vector and transform the data
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] {
                        "amount",
                        "oldbalanceOrg",
                        "newbalanceOrig",
                        "oldbalanceDest",
                        "newbalanceDest",
                        "typeIndex",
                })
                .setOutputCol("vectorizedFeatures");
        Dataset<Row> assembledData = assembler.transform(data);

        // Split the data into training and test sets (50/50)
        Dataset<Row>[] splits = assembledData.randomSplit(new double[] { 0.5, 0.5 }, 1234);
        trainingData = splits[0];
        testData = splits[1];
    }

    private static Dataset<Row> training(){
        // Train Naive Bayes model
        NaiveBayes nb = new NaiveBayes().setLabelCol("isFraud").setFeaturesCol("vectorizedFeatures");
        NaiveBayesModel model = nb.fit(trainingData);

        // Make predictions on test data
        return model.transform(testData);
    }

    private static Double evaluation(Dataset<Row> predictions){
        // Evaluate the model
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("isFraud")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        
        // Calculate the accuracy of the model
        return evaluator.evaluate(predictions);
    }
}
