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

package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarAlias(child: Expression, name: String)(
    override val exprId: ExprId,
    override val qualifier: Seq[String],
    override val explicitMetadata: Option[Metadata])
    extends Alias(child, name)(exprId, qualifier, explicitMetadata)
    with ColumnarExpression {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
  }

}

class ColumnarAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId,
    override val qualifier: Seq[String])
    extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    with ColumnarExpression {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    val field = Field.nullable(s"${name}#${exprId.id}", resultType)
    (TreeBuilder.makeField(field), resultType)
  }

}
