package io.confluent.developer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

import io.confluent.developer.avro.Movie;
import io.confluent.developer.avro.RatedMovie;
import io.confluent.developer.avro.Rating;

import java.util.Properties;

import static io.confluent.developer.MovieJoinerApp.ratedMovieAvroSerde;

public class MovieProcessor {
    public static Topology buildTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String movieTopic = props.getProperty("movie.topic.name");
        final String rekeyedMovieTopic = props.getProperty("rekeyed.movie.topic.name");
        final String ratingTopic = props.getProperty("rating.topic.name");
        final String ratedMoviesTopic = props.getProperty("rated.movies.topic.name");

        KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
                .map((key, movie) -> new KeyValue<>(String.valueOf(movie.getId()), movie));

        movieStream.to(rekeyedMovieTopic);

        KTable<String, Movie> movies = builder.table(rekeyedMovieTopic);

        KStream<String, Rating> ratings = builder.<String, Rating>stream(ratingTopic)
                .map((key, rating) -> new KeyValue<>(String.valueOf(rating.getId()), rating));

        KStream<String, RatedMovie> ratedMovie = ratings.join(movies, new MovieRatingJoiner());

        ratedMovie.to(ratedMoviesTopic, Produced.with(Serdes.String(), ratedMovieAvroSerde(props)));

        return builder.build();
    }
}

class MovieRatingJoiner implements ValueJoiner<Rating, Movie, RatedMovie> {

    public RatedMovie apply(Rating rating, Movie movie) {
        return RatedMovie.newBuilder()
                .setId(movie.getId())
                .setTitle(movie.getTitle())
                .setReleaseYear(movie.getReleaseYear())
                .setRating(rating.getRating())
                .build();
    }
}
