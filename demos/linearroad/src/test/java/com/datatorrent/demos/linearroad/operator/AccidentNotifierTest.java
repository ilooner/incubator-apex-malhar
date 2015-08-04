/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.linearroad.operator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class AccidentNotifierTest
{
  public static class TestMeta extends TestWatcher
  {
    AccidentNotifier operator;
    CollectorTestSink<Object> accidentReport;

    @Override
    protected void starting(Description description)
    {
      accidentReport = new CollectorTestSink<Object>();
      operator = new AccidentNotifier();
      operator.setup(null);
      operator.accidentNotification.setSink(accidentReport);
    }

    @Override
    protected void finished(Description description)
    {
      operator.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testOperator() throws Exception
  {
    testMeta.operator.beginWindow(1);
    PositionReport report = new PositionReport(420, 12, 34, 0, 1, 1, 54, 285120);
    //(int eventTime, int vehicleId, int vehicleSpeed, int expressWayId, int lane, int direction, int segment, int position)
    testMeta.operator.positionReport.process(report);
    testMeta.operator.accidentDetectedReport.process(new AccidentDetector.AccidentDetectTuple(new AccidentDetector.AccidentKey(report), 360));
    testMeta.operator.endWindow();
    Assert.assertEquals("One output expected", 1, testMeta.accidentReport.collectedTuples.size());
  }
}
