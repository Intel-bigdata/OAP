#!/bin/bash

SPARK_UPSTREAM_URL="https://github.com/apache/spark/archive/"
SPARK_VERSION_SUPPORTED=(
  "v2.3.2"
)

CUR_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
OAP_HOME=$(dirname $CUR_DIR)
DOWNLOADED_SPARK_DIR=$(dirname $OAP_HOME)
PATCH_DIR=$OAP_HOME/patches

function echo_usage() {
  cat << EOF
  Usage:
    ${0} -c <path>
    ${0} -v <spark_version>
    -c | --custom-spark   - OPTIONAL. Patch to the customed spark. It you don't set, we will download from upstream
    -v | --version        - REQUIRED. Patch to the upstream spark, current we support ${SPARK_VERSION_SUPPORTED}
                            Also you can select one of version and try to apply to your spark

    For example:
      apply_patch.sh -v v2.3.2
      apply_patch.sh -v v2.3.2 -c YOUR_SPARK_DIR
EOF
exit 1
}

CURL=$(command -v curl)
check_curl_exist() {
  if ! [ -x "$CURL" ]; then
    echo "ERROR: curl is not found!"
    exit 1
  fi
}

GIT=$(command -v git)
check_git_exist() {
  if ! [ -x "$GIT" ]; then
    echo "ERROR: git is not found!"
    exit 1
  fi
}

TARBALL_SUFFIX=".tar.gz"
fetch_from_upstream() {
  check_curl_exist

  TARBALL_NAME=$UPSTREAM_SPARK_VERSION$TARBALL_SUFFIX
  mkdir -p $DOWNLOADED_SPARK_DIR/spark_source
  $CURL --fail -L $SPARK_UPSTREAM_URL$TARBALL_NAME \
          -o $TARBALL_NAME
  if [ $? != 0 ]; then
    echo "Please check whether the spark version is in support list ($SPARK_VERSION_SUPPORTED) and ensure the network available"
    exit 1
  fi

  SPARK_SOURCE_NAME=`tar -tf $TARBALL_NAME | head -1 | cut -f1 -d"/"`
  if [ $? != 0 ]; then
    exit 1

  fi
  SPARK_SOURCE_DIR=$DOWNLOADED_SPARK_DIR/spark_source/$SPARK_SOURCE_NAME
  if [ -d $SPARK_SOURCE_DIR ]; then
    rm -rf $SPARK_SOURCE_DIR
  fi
  tar -zxf $TARBALL_NAME -C $DOWNLOADED_SPARK_DIR/spark_source
  rm $TARBALL_NAME
}

patch_to_spark() {
  # TODO: we'd better use /usr/bin/patch to apply patch. Here seems the current patch file is not applicable
  check_git_exist
  cd $SPARK_SOURCE_DIR
  echo $SPARK_SOURCE_DIR

  if ! [ -d .git ]; then
    $GIT init
  fi

  echo '1111111111111111111111'
  echo $PATCH_DIR
  echo '22222222222222222222222222222'
  echo $UPSTREAM_SPARK_VERSION
  $GIT apply $PATCH_DIR/$UPSTREAM_SPARK_VERSION/*.patch
  echo $PATCH_DIR/$UPSTREAM_SPARK_VERSION
  if [ $? != 0 ]; then
    echo "Fail to apply the patch to spark. Please try to solve conflicts if you are using custom spark."
    exit 1
  fi
  echo "Apply patches to spark successfully."
}

copy_to_oap() {
  mkdir -p $OAP_HOME/patched_file
  PATHED_FILE_DIR=$OAP_HOME/patched_file
  PATCHED_FILES=`grep -r "git" $PATCH_DIR/$UPSTREAM_SPARK_VERSION/* | awk '{print substr($3,2)}'`
  for FILE in $PATCHED_FILES
  do
    cp $SPARK_SOURCE_DIR$FILE $PATHED_FILE_DIR
  done
}

###################################################################################

OPTS=`getopt --options c:v: --long "custom-spark:,version:" -n "$0" -- "$@"`
if [ $? != 0 ]; then
  echo "Failed parsing options."
  echo_usage
  exit 1
fi

while true; do
  case "$1" in
    -c | --custom-spark)
      SPARK_SOURCE_DIR=$2; shift 2;;
    -v | --version)
      UPSTREAM_SPARK_VERSION=$2; shift 2;;
    --)
      shift; break;;
    *)
      break ;;
  esac
done

if [ -z "$UPSTREAM_SPARK_VERSION" ]; then
  echo_usage
fi

if [[ "${SPARK_VERSION_SUPPORTED[*]}" =~ $UPSTREAM_SPARK_VERSION ]]; then
  fetch_from_upstream
fi

patch_to_spark
copy_to_oap
