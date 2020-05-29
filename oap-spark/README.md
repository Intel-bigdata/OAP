
# How to contribute

OAP Spark packages includes all Spark changed files. All codes are directly copied from
https://github.com/Intel-bigdata/Spark. Please make sure all your changes are committed to the
repository above. Otherwise, your change will be override by others.

The files from this package should avoid depending on other OAP module except OAP-Common.

All Spark source code changes are tracked in dev/changes_list/spark_changed_files

All changed files are ordered by file name.

You can execute the script dev/Apply_Spark_changes.sh with the specified Spark source directories
and OAP source directories accordingly.
