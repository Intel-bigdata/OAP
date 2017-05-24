/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor.custom

import org.apache.spark._

/**
 * User can extends the Trait to implement method `status`, after that, user can add a
 * configuration of `spark.executor.customInfoClass` to identify the class that user defined.
 */
trait CustomManager {
  /**
   * get the status for users long run service, the status' format is a string which includes
   * all infos. The string can be a Json string.
   * @param conf take SparkConf as input for user to handle
   * @return
   */
  def status(conf: SparkConf): String
}
