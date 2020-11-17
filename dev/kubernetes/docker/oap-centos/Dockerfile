# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM spark-centos:1.0.0

MAINTAINER The Optimized Analyitics Package for Spark Platform (OAP) Authors https://github.com/Intel-bigdata/OAP/

#oap
ARG OAP_VERSION=0.9.0
ENV OAP_VERSION                       ${OAP_VERSION}
ENV OAP_HOME              /opt/home/conda/envs/oap-${OAP_VERSION}

USER root

#install runtime prerequisites
RUN git clone https://github.com/Intel-bigdata/OAP oap_source && \
  cd /opt/home/oap_source && \
  git checkout v0.9.0-spark-3.0.0 && \
  sh /opt/home/oap_source/dev/install-runtime-dependencies.sh --with-rdma && \
  rm -rf /opt/home/oap_source
  
# Install OAP conda packages
COPY oap.yml /opt/home/oap.yml
RUN /opt/home/conda/bin/conda config --system --set channel_priority flexible && \
    /opt/home/conda/bin/conda env create --file /opt/home/oap.yml

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
ENV PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig/:${PKG_CONFIG_PATH}


