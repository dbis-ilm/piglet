#!/bin/bash

TRUE=0
FALSE=1

PIGLET_HOME=.

FILES=(target/scala-2.11/piglet.jar
 sparklib/target/scala-2.11/sparklib_2.11-*.jar
 flinklib/target/scala-2.11/flinklib_2.11-*.jar
 common/target/scala-2.11/common_2.11-*.jar
 ceplib/target/scala-2.11/ceplib_2.11-*.jar
 mapreducelib/target/scala-2.11/mapreduce_2.11-*.jar
 script/piglet)

TARGET_DIR=$PIGLET_HOME/piglet-dist


function checkfile {
  # echo "checking file $1"
  if [ -r $1 ]; then
    FEXISTS=TRUE
  else
    FEXISTS=FALSE
  fi

}

if [ -z "$PIGLET_HOME" ]; then
  echo "Please set PIGLET_HOME"
  exit 1
fi

rm -rf  $TARGET_DIR
mkdir $TARGET_DIR

for f in ${FILES[@]}
do
  echo -ne "\r copying $f                                                                                              "
  sourcefile=$PIGLET_HOME/$f
  checkfile $sourcefile
  if [ $FEXISTS == TRUE ]; then
    # targetfile=$TARGET_DIR/$f
    cp --parents $sourcefile $TARGET_DIR
  else
    echo "File $f does not exist - aborting"
    rm -rf $TARGET_DIR
    exit 1
  fi
done
echo -e "\rcopied files                                                                                                "

echo -n "creating archive..."
tar jcf ${TARGET_DIR}.tar.bz2 ${TARGET_DIR}
echo " finished --> ${TARGET_DIR}.tar.bz2"

echo "cleanup"
rm -rf $TARGET_DIR
