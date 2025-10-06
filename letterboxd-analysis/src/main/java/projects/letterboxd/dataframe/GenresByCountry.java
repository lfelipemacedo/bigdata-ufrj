package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.struct;

public class GenresByCountry extends DataFrame {
    private Dataset<Row> genresByCountry;

    @Override
    void build() {
        genresByCountry = genresDf.join(countriesDf, "id")
                .groupBy("country", "genre")
                .agg(countDistinct("id").alias("count"))
                .withColumn("genre_info", struct(col("genre"), col("count")))
                .groupBy("country")
                .agg(collect_list(col("genre_info")).alias("genres"))
                .orderBy("country");
    }

    @Override
    void save() {
        genresByCountry.write()
                .format("mongodb")
                .option("collection", "genres_by_country")
                .mode("overwrite")
                .save();
    }
}
