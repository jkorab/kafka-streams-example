package com.ameliant.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author jkorab
 */
@Component("wordCountStream")
public class WordCountStream {

    private String topic = "lines-0.1";
    private KafkaStreams streams;

    @PostConstruct
    public void runStream() {
        Serde<String> stringSerde = Serdes.String();

        Properties config = new Properties();
        final String version = "0.2";
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example-" + version);
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        KStreamBuilder wordLines = new KStreamBuilder();
        wordLines.stream(stringSerde, stringSerde, topic)
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .filter((key, value) -> value.trim().length() > 0)
                .map((key, value) -> new KeyValue<>(value, value))
                .countByKey("counts")
                .toStream()
                .to(stringSerde, Serdes.Long(), "words-with-counts-" + version);

        streams = new KafkaStreams(wordLines, config);
        streams.start();
    }

    @PreDestroy
    public void closeStream() {
        streams.close();
    }
}
