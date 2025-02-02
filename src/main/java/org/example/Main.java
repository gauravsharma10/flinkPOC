package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         // creating kafka connectors
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-consumer-group");
        FlinkKafkaConsumer<String> kafkaTopicA = new FlinkKafkaConsumer<>("TopicA", new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<String> kafkaTopicB = new FlinkKafkaConsumer<>("TopicB", new SimpleStringSchema(), properties);

        FlinkKafkaProducer<String> kafkaTopicC = new FlinkKafkaProducer<>("TopicC", new SimpleStringSchema(), properties);
        FlinkKafkaProducer<String> kafkaTopicD = new FlinkKafkaProducer<>("TopicD", new SimpleStringSchema(), properties);

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(0))
            .withIdleness(java.time.Duration.ofMillis(1))
            .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

        // consuming kafka data into streams
        DataStream<Tuple2<String, String>> parsedStreamA = env.addSource(kafkaTopicA)
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String value) throws Exception {
                    String tagBValue = value.split("<TagB>")[1].split("</TagB>")[0];
                    LOG.info("Extracted TagB value from Topic A: {}", tagBValue);
                    return new Tuple2<>(tagBValue, value);
                }
            });

        // consuming kafka data into streams
        DataStream<Tuple2<String, String>> parsedStreamB = env.addSource(kafkaTopicB)
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String value) throws Exception {
                    String[] parts = value.split(",");
                    LOG.info("Received data in TOPIC B: " + value);
                    return new Tuple2<>(parts[0], parts[1]); // Assuming the format is TagB_value, Enriched_value
                }
            });

        // doing join operation for enriched data
        DataStream<String> enrichedData = parsedStreamA
            .join(parsedStreamB)
            .where(tuple -> tuple.f0)
            .equalTo(tuple -> tuple.f0)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .trigger(ProcessingTimeTrigger.create())
            .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                @Override
                public String join(Tuple2<String, String> streamARecord, Tuple2<String, String> streamBRecord) {
                    String enrichedPayload = streamARecord.f1.replace("<TagB>" + streamARecord.f0 + "</TagB>",
                        "<TagB>" + streamBRecord.f1 + "</TagB>");
                    LOG.info("Enriched payload: {}", enrichedPayload);
                    return enrichedPayload;
                }
            });

        enrichedData.addSink(kafkaTopicC);

        // doing cogroup operation for non enriched data
        DataStream<String> notMatchedData = parsedStreamA.coGroup(parsedStreamB)
            .where(tuple -> tuple.f0)
            .equalTo(tuple -> tuple.f0)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .trigger(ProcessingTimeTrigger.create())
            .apply(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                @Override
                public void coGroup(Iterable<Tuple2<String, String>> streamARecords, Iterable<Tuple2<String, String>> streamBRecords, Collector<String> out) {
                    if (!streamBRecords.iterator().hasNext()) {
                        for (Tuple2<String, String> record : streamARecords) {
                            LOG.info("Missing mapping for TagB: {}", record.f0);
                            out.collect(record.f1);
                        }
                    }
                }
            });

        notMatchedData.addSink(kafkaTopicD);


        env.execute("Flink Job");
    }
}
