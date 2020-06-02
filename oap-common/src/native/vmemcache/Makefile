#/*
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *    http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

ifeq ($(VERBOSE),1)
ECHO :=
else
ECHO := @
endif

# A function to check whether every environment variable in a list is defined.
check = $(foreach var,$(1),$(if $($(var)),,$(error Please set $(var))))

TARGET := libvmemcachejni.so

MODULES :=

# Source files.
SRCS := $(foreach D,$(MODULES),$(wildcard $D/*.c)) vmemcachejni.c

# Include files.
INCLUDES  := $(addprefix -I,$(MODULES)) \
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/linux \
	-I.

LIB_DIRS :=

# Libraries.
LIBS := vmemcache

CPPFLAGS += $(INCLUDES)
CFLAGS ?= -O3
CFLAGS += -Wall
CFLAGS += -fPIC

ifeq ($(DEBUG),1)
CFLAGS += -g
endif

SONAME_FLAGS = -Wl,-soname=$(TARGET)
LDFLAGS += -shared

all: $(TARGET)
	@echo "Build native library done"

$(TARGET) : $(SRCS)
	@echo Compiling dynamic library
	$(ECHO)$(CC) $(CPPFLAGS) $(CFLAGS) \
			$(SRCS) \
			$(LDFLAGS) $(SONAME_FLAGS) \
			$(foreach D,$(LIB_DIRS),-L$D) \
			$(foreach L,$(LIBS),-l$L) \
			-o $@
	$(ECHO)cp $(TARGET) ../../resources/linux/64/lib

clean:
	$(ECHO)rm -rf *.so *.o
	@echo Cleaning completed

.PHONY : all clean
