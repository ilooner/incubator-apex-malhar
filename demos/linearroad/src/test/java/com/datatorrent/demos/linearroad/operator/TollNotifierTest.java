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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class TollNotifierTest
{
  public static class TestMeta extends TestWatcher
  {
    TollNotifier operator;
    CollectorTestSink<Object> tollNotification;
    CollectorTestSink<Object> accidentClearReport;

    @Override
    protected void starting(Description description)
    {
      tollNotification = new CollectorTestSink<Object>();
      accidentClearReport = new CollectorTestSink<Object>();
      operator = new TollNotifier();
      operator.setup(null);
      operator.tollNotification.setSink(tollNotification);
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
    testMeta.operator.positionReport.process(new PositionReport(7313, 9471, 56, 0, 1, 0, 68, 359595));
    testMeta.operator.positionReport.process(new PositionReport(7407, 9471, 0, 0, 1, 0, 68, 364319));
    testMeta.operator.endWindow();
  }
}
