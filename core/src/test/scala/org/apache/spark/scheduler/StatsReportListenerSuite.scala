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

package org.apache.spark.scheduler

import org.apache.logging.log4j.Level
import org.mockito.Mockito.{spy, verifyNoInteractions}

import org.apache.spark.{SparkFunSuite, Success}
import org.apache.spark.executor.TaskMetrics

class StatsReportListenerSuite extends SparkFunSuite {
  test("disabled stage reporting skips metrics and clears completed tasks") {
    val listener = new StatsReportListener
    val metrics = spy(TaskMetrics.empty)
    val taskInfo = new TaskInfo(0, 0, 0, 0, 0, "executor", "host", TaskLocality.ANY, false)
    taskInfo.finishTime = 10
    val taskEnd = SparkListenerTaskEnd(0, 0, "ResultTask", Success, taskInfo, null, metrics)
    val stage = new StageInfo(0, 0, "test", 1, Seq.empty, Seq.empty, "test",
      resourceProfileId = 0)
    val stageCompleted = SparkListenerStageCompleted(stage)
    val appender = new LogAppender("stage statistics")
    withLogAppender(appender, Seq(classOf[StatsReportListener].getName), Some(Level.WARN)) {
      listener.onTaskEnd(taskEnd)
      listener.onStageCompleted(stageCompleted)
      verifyNoInteractions(metrics)
    }
    withLogAppender(appender, Seq(classOf[StatsReportListener].getName), Some(Level.INFO)) {
      listener.onStageCompleted(stageCompleted)
      verifyNoInteractions(metrics)
      listener.onTaskEnd(taskEnd)
      listener.onStageCompleted(stageCompleted)
    }
    val messages = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    assert(messages.count(_.startsWith("task runtime:")) == 1)
    assert(messages.exists(_.startsWith("shuffle bytes written:")))
  }
}
