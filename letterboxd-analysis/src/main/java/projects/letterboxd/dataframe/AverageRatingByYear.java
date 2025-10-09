package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class AverageRatingByYear extends DataFrame {

    public Dataset<Row> avgByYear;

    @Override
    void build() {
        Dataset<Row> movieRatings = moviesDf
                .select(
                        col("id"),
                        col("rating").cast("double").alias("movie_rating")
                )
                .na().drop(new String[]{"movie_rating"});

        Dataset<Row> firstReleaseYear = releasesDf
                .withColumn("date_release", to_date(col("date")))
                .groupBy("id")
                .agg(min(col("date_release")).alias("first_release_date"))
                .withColumn("year", year(col("first_release_date")));

        avgByYear = firstReleaseYear
                .join(movieRatings, "id")
                .groupBy("year")
                .agg(
                        round(avg("movie_rating"), 3).alias("avg_rating"),
                        countDistinct("id").alias("n_movies")
                )
                .orderBy("year");
    }

    @Override
    void save() {
        avgByYear.write()
                .format("mongodb")
                .option("collection", "avg_rating_by_year")
                .mode("overwrite")
                .save();
    }
}
