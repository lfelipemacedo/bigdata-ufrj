package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public abstract class DataFrame {
    private final String basePath = "/opt/spark-app";
    private final String mongo = System.getenv("MONGO_DB");
    private final SparkSession spark = SparkSession.builder()
            .appName("Letterboxd Analysis")
            .master("local[*]")
            .config("spark.mongodb.write.connection.uri", mongo)
            .config("spark.mongodb.write.database", "letterboxd-analysis")
            .getOrCreate();

    protected Dataset<Row> moviesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/movies.csv");

    protected Dataset<Row> countriesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/countries.csv");

    protected Dataset<Row> actorsDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/actors.csv");

    protected Dataset<Row> crewDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/crew.csv");

    protected Dataset<Row> genresDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/genres.csv");

    protected Dataset<Row> languagesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/languages.csv");

    protected Dataset<Row> releasesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/releases.csv");

    protected Dataset<Row> studiosDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/studios.csv");

    protected Dataset<Row> themesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv(basePath + "/data/themes.csv");

    public static List<DataFrame> dataFrames() {
        return List.of(new Movies(),
                new GenresByCountry(),
                new ReleasesByCountry());
    }

    public void executeSpark() {
        build();
        save();
    }

    abstract void build();

    abstract void save();
}
