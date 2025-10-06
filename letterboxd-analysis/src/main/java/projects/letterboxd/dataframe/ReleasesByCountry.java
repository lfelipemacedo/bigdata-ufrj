package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

public class ReleasesByCountry extends DataFrame {
    Dataset<Row> releasesByCountry;

    @Override
    void build() {
        releasesByCountry = releasesDf.withColumnRenamed("date", "date_release")
                .groupBy("id", "country")
                .agg(collect_list(struct(col("date_release"), col("type"))).alias("releases"))
                .groupBy("id")
                .agg(collect_list(struct(col("country").alias("country"), col("releases").alias("releases"))).alias("releases"))
                .join(moviesDf, "id")
                .select(
                        col("id"),
                        col("name"),
                        col("releases")
                ).orderBy("id");
    }

    @Override
    void save() {
        releasesByCountry.write()
                .format("mongodb")
                .option("collection", "releases_by_country")
                .mode("overwrite")
                .save();
    }
}
