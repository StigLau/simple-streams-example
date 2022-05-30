package io.confluent.developer;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.Test;

import io.confluent.developer.avro.Movie;
import io.confluent.developer.avro.RatedMovie;
import io.confluent.developer.avro.Rating;

import static org.junit.Assert.assertEquals;

public class MovieRatingJoinerTest {

  @Test
  public void apply() {
    RatedMovie actualRatedMovie;

    Movie treeOfLife = Movie.newBuilder().setTitle("Tree of Life").setId(354).setReleaseYear(2011).build();
    Rating rating = Rating.newBuilder().setId(354).setRating(9.8).build();
    RatedMovie expectedRatedMovie = RatedMovie.newBuilder()
        .setTitle("Tree of Life")
        .setId(354)
        .setReleaseYear(2011)
        .setRating(9.8)
        .build();

    actualRatedMovie = ((ValueJoiner<Rating, Movie, RatedMovie>) (rating1, movie) -> RatedMovie.newBuilder()
            .setId(movie.getId())
            .setTitle(movie.getTitle())
            .setReleaseYear(movie.getReleaseYear())
            .setRating(rating1.getRating())
            .build())
            .apply(rating, treeOfLife);

    assertEquals(actualRatedMovie, expectedRatedMovie);
  }
}
