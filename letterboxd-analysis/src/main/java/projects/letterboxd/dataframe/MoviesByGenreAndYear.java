package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class MoviesByGenreAndYear extends DataFrame {

    private Dataset<Row> moviesByGenreAndYear;

    @Override
    void build() {

        Dataset<Row> movieGenres = genresDf
                .select(col("id"), col("genre"))
                .na().drop(new String[]{"genre"});

        Dataset<Row> releaseYears = releasesDf
                .withColumn("date_release", to_date(col("date")))
                .withColumn("year", year(col("date_release")))
                .select(col("id"), col("year"))
                .na().drop(new String[]{"year"});

        moviesByGenreAndYear = movieGenres
                .join(releaseYears, "id")
                .groupBy("genre", "year")
                .agg(countDistinct("id").alias("n_movies"))
                .orderBy(col("year"), desc("n_movies"));
    }

    @Override
    void save() {
        moviesByGenreAndYear.write()
                .format("mongodb")
                .option("collection", "movies_by_genre_and_year")
                .mode("overwrite")
                .save();
    }
}
