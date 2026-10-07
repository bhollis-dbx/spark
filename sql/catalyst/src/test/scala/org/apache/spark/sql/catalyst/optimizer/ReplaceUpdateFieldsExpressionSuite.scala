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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GetStructField, Literal,
  UpdateFields, With, WithField}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReplaceUpdateFieldsExpressionSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] =
      Batch("ReplaceUpdateFieldsExpression", FixedPoint(10),
        ReplaceUpdateFieldsExpression, RewriteWithExpression) :: Nil
  }

  private def nestedUpdates(struct: Expression, depth: Int): Expression = {
    (0 until depth).foldLeft(struct) { (current, index) =>
      UpdateFields(current, Seq(WithField("nested",
        UpdateFields(GetStructField(current, 0), Seq(WithField(s"field$index", Literal(index)))))))
    }
  }

  private def expressionNodeCount(nullable: Boolean, depth: Int): Int = {
    val nested = if (nullable) {
      $"nested".struct($"value".int)
    } else {
      $"nested".struct($"value".int).notNull
    }
    val struct = if (nullable) {
      $"struct".struct(nested, $"a".int, $"b".int, $"c".int)
    } else {
      $"struct".struct(nested, $"a".int, $"b".int, $"c".int).notNull
    }
    val plan = LocalRelation(struct)
      .select(Alias(nestedUpdates(struct, depth), "result")())
    val optimized = Optimize.execute(plan)
    assert(!optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[With]))))
    optimized.collect {
      case node => node.expressions.map(_.collect { case expression => expression }.size).sum
    }.sum
  }

  test("UpdateFields replacement grows linearly with nested update depth") {
    Seq(true, false).foreach { nullable =>
      val depthThree = expressionNodeCount(nullable, 3)
      val depthSix = expressionNodeCount(nullable, 6)
      assert(depthSix < depthThree * 3,
        s"nullable=$nullable: depth 3 has $depthThree nodes, depth 6 has $depthSix")
    }
  }
}
