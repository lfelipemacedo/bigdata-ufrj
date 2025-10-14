# Imagem base com Spark 3.5.7, Scala 2.12 e Java 17
FROM apache/spark:3.5.7-scala2.12-java17-ubuntu

# Define diretório de trabalho
WORKDIR /opt/spark-app

# Copia o JAR gerado localmente
COPY target/letterboxd-analysis-1.0-SNAPSHOT.jar letterboxd-analysis.jar

# Exponha a porta da Spark UI (opcional)
EXPOSE 4040

# Define variáveis padrão
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV MAIN_CLASS=projects.letterboxd.ExecuteSpark

# Comando padrão de execução
ENTRYPOINT ["bash", "-c", "spark-submit --master local[*] --class $MAIN_CLASS /opt/spark-app/letterboxd-analysis.jar"]
