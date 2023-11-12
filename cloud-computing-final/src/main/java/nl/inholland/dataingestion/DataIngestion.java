package nl.inholland.dataingestion;

import org.apache.spark.sql.*;

import org.apache.spark.SparkContext;

public class DataIngestion {
    // Set constants
    private static final String INPUT_FILE_PATH = "data/Fraud.csv";

    // Create variable
    private static Dataset<Row> data;

    public static void main(String[] args) {
        // Create Spark session
        SparkContext sparkContext = new SparkContext("local[*]", "FraudDataIngestion");
        sparkContext.setLogLevel("ERROR");
        SparkSession spark = SparkSession.builder().getOrCreate();

        // Read data into a DataFrame
        data = spark.read().option("header", "true").csv(INPUT_FILE_PATH);

        // PREPROCESSING
        preprocessing();

        // Show the number of rows in the DataFrame
        Long count = data.count();
        System.out.println("Number of rows: " + count);

        // Show the original data
        System.out.println("Original Data:");
        data.show();

        // Show the amount of fraudulent and non-fraudulent transactions
        Dataset<Row> fraudulentTransactions = data.where("isFraud = 1");
        System.out.println("Amount of fraudulent transactions: " + fraudulentTransactions.count());
        System.out.println(
                "Amount of non-fraudulent transactions: " + (count - fraudulentTransactions.count()));

        // Show the average amount processed of fraudulent transaction
        System.out.println("Average amount processed of fraudulent transactions: "
                + fraudulentTransactions.select("amount").groupBy().avg("amount").first().get(0));

        // Show the highest amount prosseced of fraudulent transactions
        System.out.println("Highest amount processed of fraudulent transactions: "
                + fraudulentTransactions.select("amount").groupBy().max("amount").first().get(0));

        // Show the lowest amount of fraud transaction
        System.out.println("Lowest amount processed of fraudulent transactions: "
                + fraudulentTransactions.select("amount").groupBy().min("amount").first().get(0));

        // Sort the data by isFraud and isFlaggedFraud
        data = data.sort(data.col("isFraud").desc(), data.col("isFlaggedFraud").desc());

        // Take the first 1000 rows
        Row[] firstThousand = (Row[]) data.take(1000);

        // Show first value of the first 1000 rows
        System.out.println("First value of the first 1000 rows: " + firstThousand[0]);

        // Closing the Spark session
        spark.stop();
    }

    private static void preprocessing() {
        // Specify the schema of the data
        data = data.withColumn("step", data.col("step").cast("integer"));
        data = data.withColumn("amount", data.col("amount").cast("integer"));
        data = data.withColumn("oldbalanceOrg", data.col("oldbalanceOrg").cast("integer"));
        data = data.withColumn("newbalanceOrig", data.col("newbalanceOrig").cast("integer"));
        data = data.withColumn("oldbalanceDest", data.col("oldbalanceDest").cast("integer"));
        data = data.withColumn("newbalanceDest", data.col("newbalanceDest").cast("integer"));
        data = data.withColumn("isFraud", data.col("isFraud").cast("boolean"));
        data = data.withColumn("isFlaggedFraud", data.col("isFlaggedFraud").cast("boolean"));

        // Drop rows with missing values
        data = data.na().drop();
    }
}