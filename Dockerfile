FROM java:latest
MAINTAINER stefan.hagedorn@tu-ilmenau.de

COPY /script/piglet /piglet/

COPY target/scala-2.11/PigCompiler.jar /piglet/
ENV PIG_LIB /piglet/PigCompiler.jar 

COPY /sparklib/target/scala-2.11/sparklib_2.11-*.jar /sparklib/target/scala-2.11/
ENV BACKEND_DIR /sparklib/target/scala-2.11/*

# enable these to support other backends. 
#COPY /flinklib/target/scala-2.11/flinklib_2.11-*.jar /flinklib/target/scala-2.11/
#COPY /mapreduce/target/scala-2.11/mapreduce_2.11-*.jar /mapreduce/target/scala-2.11/

ENV SPARK_JAR /piglet/spark-assembly-1.5.1-hadoop2.4.0.jar 


RUN wget -q -P /piglet http://moria.prakinf.tu-ilmenau.de/spark-assembly-1.5.1-hadoop2.4.0.jar 
#RUN mv spark-assembly-1.5.1-hadoop2.4.0.jar /piglet/


ENTRYPOINT ["/piglet/piglet"]
CMD ["--help"]
