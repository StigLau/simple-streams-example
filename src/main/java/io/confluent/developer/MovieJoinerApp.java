package io.confluent.developer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.time.Duration;

import io.confluent.developer.avro.RatedMovie;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.developer.MovieProcessor.buildTopology;

public class MovieJoinerApp {



    public static SpecificAvroSerde<RatedMovie> ratedMovieAvroSerde(Properties allProps) {
        SpecificAvroSerde<RatedMovie> movieAvroSerde = new SpecificAvroSerde<>();
        movieAvroSerde.configure((Map)allProps, false);
        return movieAvroSerde;
    }

    public void createTopics(Properties allProps) {
        AdminClient client = AdminClient.create(allProps);
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(
                allProps.getProperty("movie.topic.name"),
                Integer.parseInt(allProps.getProperty("movie.topic.partitions")),
                Short.parseShort(allProps.getProperty("movie.topic.replication.factor"))));

        topics.add(new NewTopic(
                allProps.getProperty("rekeyed.movie.topic.name"),
                Integer.parseInt(allProps.getProperty("rekeyed.movie.topic.partitions")),
                Short.parseShort(allProps.getProperty("rekeyed.movie.topic.replication.factor"))));

        topics.add(new NewTopic(
                allProps.getProperty("rating.topic.name"),
                Integer.parseInt(allProps.getProperty("rating.topic.partitions")),
                Short.parseShort(allProps.getProperty("rating.topic.replication.factor"))));

        topics.add(new NewTopic(
                allProps.getProperty("rated.movies.topic.name"),
                Integer.parseInt(allProps.getProperty("rated.movies.topic.partitions")),
                Short.parseShort(allProps.getProperty("rated.movies.topic.replication.factor"))));

        client.createTopics(topics);
        client.close();
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties allProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        allProps.load(input);
        input.close();

        return allProps;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        MovieJoinerApp ts = new MovieJoinerApp();
        Properties allProps = ts.loadEnvProperties(args[0]);
        allProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        allProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        Topology topology = buildTopology(allProps);

        ts.createTopics(allProps);

        System.out.println("Topology be like \n" +
            topology.describe());
        
        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}