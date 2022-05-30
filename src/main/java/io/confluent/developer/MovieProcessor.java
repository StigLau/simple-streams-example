package io.confluent.developer;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.developer.avro.Movie;
import io.confluent.developer.avro.RatedMovie;
import io.confluent.developer.avro.Rating;

import java.util.Map;
import java.util.Properties;

public class MovieProcessor {
    public static Topology buildTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String movieTopic = props.getProperty("movie.topic.name");
        final String rekeyedMovieTopic = props.getProperty("rekeyed.movie.topic.name");
        final String ratingTopic = props.getProperty("rating.topic.name");
        final String ratedMoviesTopic = props.getProperty("rated.movies.topic.name");

        SpecificAvroSerde<RatedMovie> movieAvroSerde = new SpecificAvroSerde<>();
        movieAvroSerde.configure((Map)props, false);

        KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
                .map((key, movie) -> new KeyValue<>(String.valueOf(movie.getId()), movie));

        movieStream.to(rekeyedMovieTopic);

        KTable<String, Movie> movies = builder.table(rekeyedMovieTopic);

        KStream<String, Rating> ratings = builder.<String, Rating>stream(ratingTopic)
                .map((key, rating) -> new KeyValue<>(String.valueOf(rating.getId()), rating));

        KStream<String, RatedMovie> ratedMovie = ratings.join(movies, (rating, movie) ->
            new RatedMovie(movie.getId(), movie.getTitle(), movie.getReleaseYear(), rating.getRating()));

        ratedMovie.to(ratedMoviesTopic, Produced.with(Serdes.String(), movieAvroSerde));

        return builder.build();
    }
}
