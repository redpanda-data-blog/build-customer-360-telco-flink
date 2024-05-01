package com.example.customer360;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.Types;

public class RedpandaToFlink{

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Initialize input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Configure Kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // Adjust for your Redpanda setup
        properties.setProperty("group.id", "flink_consumer");

        // Create a Kafka source (consumer)
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "user_documents", // Redpanda topic name
                new SimpleStringSchema(), // Deserialization schema
                properties);

// Assign the source to the environment
        DataStream<String> stream = env.addSource(consumer);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


SingleOutputStreamOperator<Row> parsedStream = stream
    .map(value -> {
        // Remove leading and trailing curly braces and split by comma to get each key-value pair
        String[] parts = value.substring(1, value.length() - 1).split(",");

        // Extract each field from the parts
        // Assuming your JSON structure does not contain nested JSON objects or arrays
        // and does not include commas within the strings
        String name = parts[0].split(":")[1].replace("\"", "").trim();
        String phoneNumber = parts[1].split(":")[1].replace("\"", "").trim();
        String serviceStartDate = parts[2].split(":")[1].replace("\"", "").trim();
        int currentBalanceSMS = Integer.parseInt(parts[3].split(":")[1].trim());
        int dataBundlesMB = Integer.parseInt(parts[4].split(":")[1].trim());

        // Create a Row of objects
        return Row.of(name, phoneNumber, serviceStartDate, currentBalanceSMS, dataBundlesMB);
    })
    .returns(Types.ROW_NAMED(
        new String[]{"name", "phoneNumber", "serviceStartDate", "currentBalanceSMS", "dataBundlesMB"},
        Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.INT
    )); // Specify the types of the fields

// Define a Schema that matches your Row data structure
Schema schema = Schema.newBuilder()
    .column("name", DataTypes.STRING())
    .column("phoneNumber", DataTypes.STRING())
    .column("serviceStartDate", DataTypes.STRING())
    .column("currentBalanceSMS", DataTypes.INT())
    .column("dataBundlesMB", DataTypes.INT())
    .build();

    // Use the Schema when creating a view
    tableEnv.createTemporaryView("user_documents", parsedStream, schema);

    // Now you can perform SQL operations on your table
    Table result = tableEnv.sqlQuery(
        "SELECT name, MAX(currentBalanceSMS) AS maxSmsBalance " +
        "FROM user_documents " +
        "GROUP BY name " +
        "ORDER BY maxSmsBalance DESC " +
        "LIMIT 10"
    );
    
    DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(result, Row.class);
    retractStream.print(); 

        env.execute("Flink Redpanda Consumer");
    }
}
