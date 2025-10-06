package projects.letterboxd.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public abstract class DataFrame {
//    private String basePath = System.getProperty("user.dir") + "/letterboxd-analysis";
    private final SparkSession spark = SparkSession.builder()
            .appName("Letterboxd Analysis")
            .master("local[*]")
            .config("spark.mongodb.write.connection.uri", "mongodb://engsoft_ufrj:123456789ES@ac-mjy99vr-shard-00-00.dhzryvz.mongodb.net:27017,ac-mjy99vr-shard-00-01.dhzryvz.mongodb.net:27017,ac-mjy99vr-shard-00-02.dhzryvz.mongodb.net:27017/?ssl=true&replicaSet=atlas-pz2lem-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
            .config("spark.mongodb.write.database", "letterboxd-analysis")
            .getOrCreate();

    protected Dataset<Row> moviesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/movies.csv");

    protected Dataset<Row> countriesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/countries.csv");

    protected Dataset<Row> actorsDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/actors.csv");

    protected Dataset<Row> crewDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/crew.csv");

    protected Dataset<Row> genresDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/genres.csv");

    protected Dataset<Row> languagesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/languages.csv");

    protected Dataset<Row> releasesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/releases.csv");

    protected Dataset<Row> studiosDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/studios.csv");

    protected Dataset<Row> themesDf = spark.read()
            .option("header", true)
            .option("inferSchema", true)
            .csv("data/themes.csv");

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
