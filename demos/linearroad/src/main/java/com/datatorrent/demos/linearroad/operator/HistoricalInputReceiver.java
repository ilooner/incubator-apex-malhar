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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.demos.linearroad.data.TollTuple;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class HistoricalInputReceiver extends AbstractFileInputOperator<TollTuple>
{
  protected transient BufferedReader br;
  private boolean emit;
  public final transient DefaultOutputPort<TollTuple> tollHistoryTuplePort = new DefaultOutputPort<TollTuple>();
  public final transient DefaultOutputPort<Boolean> readCurrentData = new DefaultOutputPort<>();

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br = null;
  }

  @Override
  protected TollTuple readEntity() throws IOException
  {
    String line = br.readLine();
    if (line != null) {
      String[] splits = line.split(",");
      return new TollTuple(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]), 0);
    }
    readCurrentData.emit(true);
    return null;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    emit = true;
  }

  @Override
  public void emitTuples()
  {
    if (emit) {
      super.emitTuples();
      emit = false;
    }
  }

  @Override
  protected void emit(TollTuple tollTuple)
  {
    tollHistoryTuplePort.emit(tollTuple);
  }
}
