package edu.supmti.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: WordCountApp <input-topic> <output-topic>");
            return;
        }

        String inputTopic = args[0];
        String outputTopic = args[1];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(inputTopic);

        textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .filter((k, v) -> v != null && !v.isEmpty())
                .groupBy((key, word) -> word)
                .count()
                .toStream()
                .mapValues(count -> count.toString())
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
