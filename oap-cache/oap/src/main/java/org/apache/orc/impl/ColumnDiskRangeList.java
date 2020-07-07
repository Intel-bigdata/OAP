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

package org.apache.orc.impl;

import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnDiskRangeList extends DiskRangeList {

  private static final Logger LOG =
          LoggerFactory.getLogger(ColumnDiskRangeList.class);


  final int columnId;
  final int currentStripe;

  public ColumnDiskRangeList(int columnId, int currentStripe, long offset, long end) {
    super(offset, end);
    this.columnId = columnId;
    this.currentStripe = currentStripe;
  }

  public static class CreateColumnRangeHelper {
    private DiskRangeList tail = null;
    private DiskRangeList head;

    public CreateColumnRangeHelper() {
    }

    public DiskRangeList getTail() {
      return this.tail;
    }

    public void add(int columnId, int currentStripe, long offset, long end) {
      DiskRangeList node = new ColumnDiskRangeList(columnId, currentStripe, offset, end);
      if (this.tail == null) {
        this.head = this.tail = node;
      } else {
        this.tail = this.tail.insertAfter(node);
      }
    }

    public DiskRangeList get() {
      return this.head;
    }

    public DiskRangeList extract() {
      DiskRangeList result = this.head;
      this.head = null;
      return result;
    }
  }
}
