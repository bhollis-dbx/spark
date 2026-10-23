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

package org.apache.spark.ml.util

import org.apache.logging.log4j.Level
import org.mockito.Mockito.{mock, verifyNoInteractions}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{Dataset, Row}

class InstrumentationSuite extends SparkFunSuite {
  test("disabled dataset logging does not create an RDD") {
    val dataset = mock(classOf[Dataset[Row]])
    withLogAppender(new LogAppender("dataset info"),
      Seq(classOf[Instrumentation].getName), Some(Level.WARN)) {
      Instrumentation.instrumented(_.logDataset(dataset))
      verifyNoInteractions(dataset)
    }
  }

  test("disabled parameter logging does not encode parameter values") {
    var encodings = 0
    val params = new Params {
      override val uid: String = "test"
      val value = new Param[String](this, "value", "test value") {
        override def jsonEncode(value: String): String = {
          encodings += 1
          super.jsonEncode(value)
        }
      }
      set(value, "test")
      override def copy(extra: ParamMap): Params = defaultCopy(extra)
    }
    val appender = new LogAppender("parameter info")
    Seq(Level.WARN, Level.INFO).foreach { level =>
      withLogAppender(appender, Seq(classOf[Instrumentation].getName), Some(level)) {
        Instrumentation.instrumented(_.logParams(params, params.params.toIndexedSeq: _*))
      }
      assert(encodings == (if (level == Level.INFO) 1 else 0))
    }
    assert(appender.loggingEvents.exists(
      _.getMessage.getFormattedMessage.contains("\"value\":\"test\"")))
  }
}
