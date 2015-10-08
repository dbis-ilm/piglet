FROM java:latest
MAINTAINER stefan.hagedorn@tu-ilmenau.de

COPY target/scala-2.11/PigCompiler.jar /piglet/
COPY /sparklib/target/scala-2.11/sparklib_2.11-1.0.jar /piglet/
COPY /common/target/scala-2.11/common_2.11-1.0.jar /piglet/

# enable these to support other backends. 
#COPY /flinklib/target/scala-2.11/flinklib_2.11-1.0.jar /piglet/
#COPY /mapreduce/target/scala-2.11/mapreduce_2.11-1.0.jar /piglet/

COPY /script/pigs /piglet/
COPY /script/pigsh /piglet/

# FIXME: This is wrong, we need to add spark-assembly here, not the sparklib!
# either the spark assembly has to be copied to the context dir (next to this docker file) before build
# or we have to download it from somewhere else. This requires to install curl/wget in the image...
ENV SPARK_JAR /piglet/sparklib_2.11-1.0.jar
ENV PIG_LIB /piglet/PigCompiler.jar
ENV BACKEND_DIR /piglet/sparklib_2.11-1.0.jar

# this is a workaround: in the actial pigs script, the common lib is not  added to the CLASSPATH, so add it before
ENV CLASSPATH /piglet/common_2.11-1.0.jar


#ENTRYPOINT /piglet/pigs

