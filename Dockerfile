FROM java:latest
MAINTAINER stefan.hagedorn@tu-ilmenau.de

COPY /script/piglet /piglet/

RUN wget -q http://cloud01.prakinf.tu-ilmenau.de/spark-assembly-1.5.1-hadoop2.4.0.jar 
RUN mv spark-assembly-1.5.1-hadoop2.4.0.jar /piglet/

ENV SPARK_JAR /piglet/spark-assembly-1.5.1-hadoop2.4.0.jar 
ENV BACKEND_DIR /piglet/sparklib_2.11-1.0.jar

COPY target/scala-2.11/PigCompiler.jar /piglet/
COPY /sparklib/target/scala-2.11/sparklib_2.11-1.0.jar /piglet/
#COPY /common/target/scala-2.11/common_2.11-1.0.jar /piglet/

# enable these to support other backends. 
#COPY /flinklib/target/scala-2.11/flinklib_2.11-1.0.jar /piglet/
#COPY /mapreduce/target/scala-2.11/mapreduce_2.11-1.0.jar /piglet/

# this is a workaround: in the actual pigs script, the common lib is not  added to the CLASSPATH, so add it before
ENV PIG_LIB /piglet/PigCompiler.jar 

#ENTRYPOINT /piglet/piglet

