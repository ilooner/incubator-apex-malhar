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

import java.io.File;
import java.util.List;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class AverageSpeedCalculatorTest
{
  public static class TestMeta extends TestWatcher
  {
    AverageSpeedCalculator operator;
    CollectorTestSink<Object> averageSpeed;

    @Override
    protected void starting(Description description)
    {
      averageSpeed = new CollectorTestSink<Object>();
      operator = new AverageSpeedCalculator();
      operator.setup(null);
      operator.averageSpeed.setSink(averageSpeed);
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
    List<String> input = FileUtils.readLines(new File("src/test/resources/AverageSpeedCalculatorInput.txt"));
    for (String str : input) {
      String[] splits = str.split(",");
      int type = Integer.parseInt(splits[0]);
      if (type == 0) {
        testMeta.operator.positionReport.process(new PositionReport(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]),
          Integer.parseInt(splits[4]), Integer.parseInt(splits[5]), Integer.parseInt(splits[6]), Integer.parseInt(splits[7]), Integer.parseInt(splits[8])));
      }
    }
    testMeta.operator.endWindow();
    Assert.assertEquals("number of tuples ", 4, testMeta.averageSpeed.collectedTuples.size());
  }

}
