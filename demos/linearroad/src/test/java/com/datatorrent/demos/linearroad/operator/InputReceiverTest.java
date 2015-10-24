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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
    InputReceiver oper = new InputReceiver();
    oper.setIgnoreHeader(true);
    oper.setDirectory("file://" + new File("src/test/resources/input").getAbsolutePath());
    oper.setNumberOfPartitions(1);
    oper = oper.definePartitions(null, null).iterator().next().getPartitionedInstance();

    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    oper.positionReport.setSink(sink);
    oper.setup(testMeta.context);
    long wid;
    for (wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.handleIdleTime();
      oper.endWindow();
    }
    oper.beginWindow(wid++);
    oper.nextEventTime.process(1);
    oper.handleIdleTime();
    oper.endWindow();
    Assert.assertTrue("Total tuples emitted is 2", 2 == sink.collectedTuples.size());
    sink.collectedTuples.clear();
    Kryo kryo = new Kryo();
    // Kryo.copy fails as it attempts to clone transient fields
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output loutput = new Output(bos);
    kryo.writeObject(loutput, oper);
    loutput.close();
    Input lInput = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    InputReceiver oper1 = kryo.readObject(lInput, InputReceiver.class);
    lInput.close();
    oper.teardown();
    oper1.positionReport.setSink(sink);
    oper1.setup(testMeta.context);
    wid = 0;
    oper1.beginWindow(wid);
    oper1.handleIdleTime();
    oper1.endWindow();
    Assert.assertTrue("Total tuples emitted is 0", 0 == sink.collectedTuples.size());
    for (int i = 1; i < 4; i++) {
      ++wid;
      oper1.beginWindow(wid);
      oper1.nextEventTime.process(1);
      oper1.handleIdleTime();
      oper1.endWindow();
    }
    Assert.assertTrue("Total tuples emitted is 6", 6 == sink.collectedTuples.size());
    sink.collectedTuples.clear();

    bos = new ByteArrayOutputStream();
    loutput = new Output(bos);
    kryo.writeObject(loutput, oper1);
    loutput.close();
    lInput = new Input(bos.toByteArray());
    oper = kryo.readObject(lInput, InputReceiver.class);
    lInput.close();
    oper1.teardown();
    oper.positionReport.setSink(sink);
    oper.setup(testMeta.context);
    wid = 0;
    for (int i = 1; i < 10; i++) {
      ++wid;
      oper.beginWindow(wid);
      oper.handleIdleTime();
      oper.endWindow();
    }
    Assert.assertTrue("Total tuples emitted is 0 and collected " + sink.collectedTuples.size(), 0 == sink.collectedTuples.size());
    for (int i = 1; i < 10; i++) {
      ++wid;
      oper.beginWindow(wid);
      oper.nextEventTime.process(1);
      oper.handleIdleTime();
      oper.endWindow();
    }
    Assert.assertTrue("Total tuples emitted is 3 and collected " + sink.collectedTuples.size(), 3 == sink.collectedTuples.size());
    oper.teardown();
  }
}
