#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# TODO maybe need to check the version of Memkind

CMAKE_MINIMUM_REQUIRED (VERSION 2.6)

PROJECT(OAP)

FIND_PACKAGE(JNI REQUIRED)

INCLUDE_DIRECTORIES(${JNI_INCLUDE_DIRS})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

SET(SOURCE_FILES org_apache_spark_unsafe_PMPlatform.cpp)

ADD_LIBRARY(pmplatform SHARED ${SOURCE_FILES})

INSTALL(TARGETS pmplatform LIBRARY DESTINATION lib)

TARGET_LINK_LIBRARIES(pmplatform memkind)
