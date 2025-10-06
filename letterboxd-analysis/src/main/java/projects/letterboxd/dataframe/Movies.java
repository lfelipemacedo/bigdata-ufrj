package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.struct;

public class Movies extends DataFrame {
    private Dataset<Row> completeMovieDf;

    @Override
    void build() {
        completeMovieDf = moviesDf.join(countriesDf.groupBy("id")
                        .agg(collect_set("country").alias("countries")), "id")
                .join(actorsDf.groupBy("id")
                        .agg(collect_list(struct(col("name"), col("role"))).alias("actors")), "id")
                .join(crewDf.groupBy("id")
                        .agg(collect_list(struct(col("role"), col("name"))).alias("crew")), "id")
                .join(genresDf.groupBy("id")
                        .agg(collect_set("genre").alias("genres")), "id")
                .join(languagesDf.groupBy("id")
                        .agg(collect_list(struct(col("type"), col("language"))).alias("languages")), "id")
                .join(releasesDf.groupBy("id")
                        .agg(collect_list(struct(
                                col("country"),
                                col("date"),
                                col("type"),
                                col("rating")
                        )).alias("releases")), "id")
                .join(studiosDf.groupBy("id")
                        .agg(collect_set("studio").alias("studios")), "id")
                .join(themesDf.groupBy("id")
                        .agg(collect_set("theme").alias("themes")), "id")
                .orderBy("id");
    }

    @Override
    void save() {
        completeMovieDf.write()
                .format("mongodb")
                .option("collection", "movies")
                .mode("overwrite")
                .save();
    }
}
