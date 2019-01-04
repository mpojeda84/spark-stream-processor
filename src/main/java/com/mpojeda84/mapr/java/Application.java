package com.mpojeda84.mapr.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;
import java.util.*;

public class Application {


    public static void main(String[] args) throws InterruptedException {

        System.out.println("###### RUNNING #######");

        SparkConf sparkConf = new SparkConf().setAppName("Likes Stream Reader");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(2000));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Arrays.asList("/user/mapr/Streams/test:test-1"), getKafkaParameters()));

        stream.map(ConsumerRecord::value).print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    static Map<String, Object> getKafkaParameters() {
        HashMap map = new HashMap();
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregators");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return map;
    }
}
