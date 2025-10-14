package projects.letterboxd;

import projects.letterboxd.dataframe.DataFrame;

public class ExecuteSpark {
    public static void main(String[] args) {
        DataFrame.dataFrames().forEach(DataFrame::executeSpark);
    }
}
