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

package org.apache.spark.api.python

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.Level
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.internal.config.Python.PYTHON_LOG_INFO

class PythonUtilsSuite extends SparkFunSuite {
  test("disabled Python info logging does not launch subprocesses") {
    withTempDir { dir =>
      val calls = dir.toPath.resolve("calls")
      val executable = dir.toPath.resolve("python")
      Files.writeString(executable,
        s"#!/bin/sh\necho called >> '$calls'\necho test-python\n")
      assert(executable.toFile.setExecutable(true))
      val originalEnv = SparkEnv.get
      val env = mock(classOf[SparkEnv])
      when(env.conf).thenReturn(new SparkConf(false).set(PYTHON_LOG_INFO, true))
      SparkEnv.set(env)
      try {
        val appender = new LogAppender("Python info")
        withLogAppender(appender,
          Seq("org.apache.spark.api.python.PythonUtils"), Some(Level.WARN)) {
          PythonUtils.logPythonInfo(executable.toString)
          assert(!Files.exists(calls))
        }
        withLogAppender(appender,
          Seq("org.apache.spark.api.python.PythonUtils"), Some(Level.INFO)) {
          PythonUtils.logPythonInfo(executable.toString)
          assert(Files.readAllLines(calls).asScala.toSeq == Seq("called", "called"))
        }
        assert(appender.loggingEvents.exists(
          _.getMessage.getFormattedMessage.contains("Python version: test-python")))
      } finally {
        SparkEnv.set(originalEnv)
      }
    }
  }
}
