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
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class InputReceiverTest
{

  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH, dir);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void test() throws Exception
  {
    HashSet<String> allLines = Sets.newHashSet();
    for (int file = 0; file < 2; file++) {
      HashSet<String> lines = Sets.newHashSet();
      for (int line = 0; line < 2; line++) {
        lines.add("0,1,1,2,2,2,22,2,2,2,2," + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    InputReceiver oper = new InputReceiver();
//    oper.setScanIntervalMillis(0);
//    oper.setScanner(new InputReceiver.CustomDirectoryScanner());

    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    oper.positionReport.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.setup(testMeta.context);
    long wid;
    for (wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.handleIdleTime();
      oper.endWindow();
    }
    oper.beginWindow(wid);
    oper.startScanning.process(true);
    oper.endWindow();
    oper.beginWindow(++wid);
    oper.handleIdleTime();
    oper.handleIdleTime();
    oper.handleIdleTime();
    oper.endWindow();
    Assert.assertTrue("Total tuples emitted is 4", 4 == sink.collectedTuples.size());
    oper.teardown();
  }
}
