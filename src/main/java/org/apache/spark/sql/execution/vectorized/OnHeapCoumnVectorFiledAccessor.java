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
package org.apache.spark.sql.execution.vectorized;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * It's a helper class for OnHeapColumnVector that helps
 * to modify private variable of OnHeapColumnVector by reflection.
 */
public class OnHeapCoumnVectorFiledAccessor {

  private static final Map<String, Field> FIELD_MAP = Maps.newHashMap();

  static {
    for (Class<?> clazz = OnHeapColumnVector.class;
         clazz != Object.class; clazz = clazz.getSuperclass()) {
      Field[] declaredFields = clazz.getDeclaredFields();
      for (Field field : declaredFields) {
        makeAccessible(field);
        FIELD_MAP.put(field.getName(), field);
      }
    }
  }

  public static Object getFieldValue(final OnHeapColumnVector vector, final String fieldName) {
    Field field = FIELD_MAP.get(fieldName);

    if (field == null) {
      throw new IllegalArgumentException(
        "Could not find field [" + fieldName + "] on target [" + vector + ']');
    }

    try {
      return field.get(vector);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setFieldValue(
      final OnHeapColumnVector vector,
      final String fieldName,
      final Object value) {
    Field field = FIELD_MAP.get(fieldName);
    if (field == null) {
      throw new IllegalArgumentException(
        "Could not find field [" + fieldName + "] on target [" + vector + ']');
    }

    try {
      field.set(vector, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void makeAccessible(Field field) {
    if (!field.isAccessible() && (!Modifier.isPublic(field.getModifiers())
      || !Modifier.isPublic(field.getDeclaringClass().getModifiers())
      || Modifier.isFinal(field.getModifiers()))) {
      field.setAccessible(true);
    }
  }
}
