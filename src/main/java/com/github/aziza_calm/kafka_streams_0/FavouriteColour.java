package com.github.aziza_calm.kafka_streams_0;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class FavouriteColour {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> favColinput = builder.stream("favcol-input");

        KStream<String, String> lol = favColinput
                .filter((key, value) -> value.contains(","))
                .mapValues(value -> value.toLowerCase())
                .map((key, value) -> {
                    String[] keyval = value.split(" ");
                    if (keyval.length > 1) {
                        KeyValue<String, String> result = KeyValue.pair(keyval[0], keyval[1]);
                        return result;
                    }
                    return KeyValue.pair("null", "null");
                })
                .filter((key, value) -> (value != "green" && value != "red" && value != "blue"));
        lol.to("temp-topic");
        KTable<String, String> mytable = builder.table("temp-topic");
        KTable<String, Long> finisch = mytable
                .groupBy((key, value) -> new KeyValue<>(value, 1))
                .count();
        finisch.toStream().to("favcol-output", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        //printed the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
