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

import sys

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import PCA
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PCAExample")\
        .getOrCreate()

    if (len(sys.argv) != 3) :
        print("bin/spark-submit pca-pyspark.py <data_set.csv> <param_K>")
        sys.exit(1)    

    input = spark.read.load(sys.argv[1], format="csv", inferSchema="true", header="false")
    K = int(sys.argv[2])    
        
    assembler = VectorAssembler(
        inputCols=input.columns,
        outputCol="features")

    dataset = assembler.transform(input)   
    dataset.show() 

    pca = PCA(k=K, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(dataset)

    print("Principal Components: ", model.pc, sep='\n')
    print("Explained Variance: ", model.explainedVariance, sep='\n')

    spark.stop()
