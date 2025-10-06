package projects.letterboxd;

import projects.letterboxd.dataframe.DataFrame;

public class ExecuteSpark {
    public static void main(String[] args) {
        // Estúdios mais populares por país
        // Distribuições de filmes por país
        // Distribuição de generos por país

        DataFrame.dataFrames().forEach(DataFrame::executeSpark);
    }
}
