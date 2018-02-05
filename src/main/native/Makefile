#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# A function designed to check whether every environment variable in a list is
# defined.

.PHONY: all jni

check = $(foreach var,$(1),$(if $($(var)),,$(error Please set $(var))))

# The native part of the Java classes has one external dependency, namely the
# org_apache_hadoop.h header in the Hadoop source code. You will need to set
# HADOOPSRC to the root of your local Hadoop repository. The Cloudera mirror of
# this repository is at https://github.com/cloudera/hadoop-common.git.

# These were the compiler flags used when the native part was built into the
# Hadoop native library. It has been decoupled since, so one or more of these
# flags might be redundant.
FLAGS = -g -Wall -Werror -O2 -D_REENTRANT -D_GNU_SOURCE -D_LARGEFILE_SOURCE \
        -D_FILE_OFFSET_BITS=64

INC = -I $(JAVA_HOME)/include \
      -I $(JAVA_HOME)/include/linux \
      -I javah \
      -I .

all: libIntelCompressionCodec.so
	@echo "build native library done"

libIntelCompressionCodec.so: ZstdJNI.o
	gcc -fpic -shared -o libIntelCompressionCodec.so ZstdJNI.o

ZstdJNI.o: Zstd/ZstdJNI.c jni
	$(call check,JAVA_HOME )
	gcc -fpic $(FLAGS) $(INC) -c Zstd/ZstdJNI.c

jni:
	$(call check,JAVA_HOME)
	$(JAVA_HOME)/bin/javah -d javah -cp ../../../target/classes \
	com.intel.compression.core.IntelCompressionCodecNative

clean:
	rm -rf libIntelCompressionCodec.so \
	  javah ZstdJNI.o
