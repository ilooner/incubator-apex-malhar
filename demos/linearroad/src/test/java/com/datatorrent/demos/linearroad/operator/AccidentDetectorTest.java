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

import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class AccidentDetectorTest
{

  public static class TestMeta extends TestWatcher
  {
    AccidentDetector operator;
    CollectorTestSink<Object> accidentReport;
    CollectorTestSink<Object> accidentClearReport;

    @Override
    protected void starting(Description description)
    {
      accidentReport = new CollectorTestSink<Object>();
      accidentClearReport = new CollectorTestSink<Object>();
      operator = new AccidentDetector();
      operator.setup(null);
      operator.accidentDetectedReport.setSink(accidentReport);
      operator.accidentClearReport.setSink(accidentClearReport);
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

    int time = 1;
    for (int i = 0; i < 4; i++) {
      //PositionReport(int eventTime, int vehicleId, int vehicleSpeed, int expressWayId, int lane, int direction, int segment, int position)
      testMeta.operator.positionReport.process(new PositionReport(time, 1, 1, 1, 1, 1, 1, 1));
      testMeta.operator.positionReport.process(new PositionReport(time, 2, 1, 1, 1, 1, 1, 1));
      time += 30;
    }
    testMeta.operator.positionReport.process(new PositionReport(time, 2, 1, 1, 1, 1, 1, 1));
    Assert.assertTrue("One accident detected ", testMeta.accidentReport.collectedTuples.size() == 1);
    testMeta.operator.positionReport.process(new PositionReport(time, 2, 1, 1, 1, 1, 1, 2));
    Assert.assertTrue("One accident clear detected ", testMeta.accidentClearReport.collectedTuples.size() == 1);
    testMeta.operator.endWindow();
  }
}
