package org.apache.spark.sql.execution.datasources.parquet;


import org.apache.spark.sql.catalyst.InternalRow;

public class TestBufferedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
   private scala.collection.Iterator[] inputs;
   private scala.collection.Iterator scan_input;

   public void init(int index, scala.collection.Iterator[] inputs) {
     partitionIndex = index;
     this.inputs = inputs;
     scan_input = inputs[0];

   }

   protected void processNext() throws java.io.IOException {
     while (scan_input.hasNext()) {
       InternalRow scan_row = (InternalRow) scan_input.next();
       append(scan_row);
       if (shouldStop()) return;
     }
   }
 }
