/*******************************************************************************
* Copyright 2020 Intel Corporation
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/

#include <daal.h>
#include "org_apache_spark_ml_util_OneDAL__.h"

using namespace daal;
using namespace daal::data_management;

/*
 * Class:     org_apache_spark_ml_util_OneDAL__
 * Method:    setNumericTableValue
 * Signature: (JIID)V
 */
JNIEXPORT void JNICALL Java_org_apache_spark_ml_util_OneDAL_00024_setNumericTableValue
  (JNIEnv *, jobject, jlong numTableAddr, jint row, jint column, jdouble value) {

  HomogenNumericTable<double> * nt = static_cast<HomogenNumericTable<double> *>(((SerializationIfacePtr *)numTableAddr)->get());
  (*nt)[row][column]               = (double)value;

}

