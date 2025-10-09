package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class AverageRatingByCountry extends DataFrame {

    private Dataset<Row> avgByCountry;

    @Override
    void build() {
        Dataset<Row> movieRatings = moviesDf
                .select(
                        col("id"),
                        col("rating").cast("double").alias("movie_rating")
                )
                .na().drop(new String[]{"movie_rating"});

        avgByCountry = releasesDf
                .select(col("id"), col("country"))
                .join(movieRatings, "id")
                .groupBy("country")
                .agg(
                        round(avg("movie_rating"), 3).alias("avg_rating"),
                        countDistinct("id").alias("n_movies")
                )
                .orderBy(desc("avg_rating"));
    }

    @Override
    void save() {
        avgByCountry.write()
                .format("mongodb")
                .option("collection", "avg_rating_by_country")
                .mode("overwrite")
                .save();
    }
}
