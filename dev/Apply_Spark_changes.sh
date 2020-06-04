#!/bin/bash

function usage(){
  printf "./Apply_Spark_changes.sh [Your Spark Source Root Dir] [OAP Source Root Dir]"
}

function move_single_file (){
  rp=`echo "$1" | awk '{split($0, a, "src"); print a[2]}'`
  fp="$2$rp"
  mkdir -p "`dirname $fp`"
  cp $1 $fp
  echo "Move $1 to $fp \n"
}

function move_files_from_list_to_oap(){
  while IFS= read -r line
  do
    echo "## Processing $line"
    bn=`basename $line`
    file_path=`find $SPARK_SOURCE_DIR -name $bn | grep $line`
    [ -z "$file_path" ] || move_single_file "$file_path" $2
  done < "$1"
}

if [ "$#" -ne 2 ];then
  usage
fi

SPARK_SOURCE_DIR="$1"
OAP_SOURCE_DIR="$2"
FILE_DIR="${OAP_SOURCE_DIR}/dev/changes_list"
SPARK_FILE_LIST="${FILE_DIR}/spark_changed_files"
OAP_SPARK_SOURCE_DIR="${OAP_SOURCE_DIR}/oap-spark/src/"

move_files_from_list_to_oap $SPARK_FILE_LIST $OAP_SPARK_SOURCE_DIR
