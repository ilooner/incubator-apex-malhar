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
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

import com.datatorrent.demos.linearroad.data.AccountBalanceQuery;
import com.datatorrent.demos.linearroad.data.DailyBalanceQuery;
import com.datatorrent.demos.linearroad.data.LinearRoadTuple;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class InputReceiver extends AbstractFileInputOperator<LinearRoadTuple> implements Operator.IdleTimeHandler
{
  protected transient BufferedReader br;
  private boolean historicalScanFinished;
  private boolean startScanningData = false;

  public boolean isStartScanningData()
  {
    return startScanningData;
  }

  public void setStartScanningData(boolean startScanningData)
  {
    this.startScanningData = startScanningData;
  }

  public final transient DefaultOutputPort<PositionReport> positionReport = new DefaultOutputPort<PositionReport>();
  public final transient DefaultOutputPort<DailyBalanceQuery> dailyBalanceQuery = new DefaultOutputPort<DailyBalanceQuery>();
  public final transient DefaultOutputPort<AccountBalanceQuery> accountBalanceQuery = new DefaultOutputPort<AccountBalanceQuery>();

  public boolean isHistoricalScanFinished()
  {
    return historicalScanFinished;
  }

  public void setHistoricalScanFinished(boolean historicalScanFinished)
  {
    this.historicalScanFinished = historicalScanFinished;
  }

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
  public void handleIdleTime()
  {
    emitTuples();
  }

  @Override
  protected LinearRoadTuple readEntity() throws IOException
  {
    String line = br.readLine();
    if (line != null) {
      String[] splits = line.split(",");
      int type = Integer.parseInt(splits[0]);
      if (type == 0) {
        return new PositionReport(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]),
          Integer.parseInt(splits[4]), Integer.parseInt(splits[5]), Integer.parseInt(splits[6]), Integer.parseInt(splits[7]), Integer.parseInt(splits[8]));
      }
      if (type == 2) {
        return new AccountBalanceQuery(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[9]));
      }
      if (type == 3) {
        return new DailyBalanceQuery(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[9]), Integer.parseInt(splits[4]), Integer.parseInt(splits[14]));
      }
      if (type == 4) {
        return new LinearRoadTuple(4, 1);
      }
    }
    return null;
  }

  @Override
  protected void emit(LinearRoadTuple tuple)
  {
    if (tuple instanceof PositionReport) {
      positionReport.emit((PositionReport) tuple);
    }
    else if (tuple instanceof DailyBalanceQuery) {
      dailyBalanceQuery.emit((DailyBalanceQuery) tuple);
    }
    else if (tuple instanceof AccountBalanceQuery) {
      accountBalanceQuery.emit((AccountBalanceQuery) tuple);
    }
  }

  public final transient DefaultInputPort<Boolean> startScanning = new DefaultInputPort<Boolean>()
  {
    @Override
    public void process(Boolean aBoolean)
    {
      historicalScanFinished = aBoolean;
    }
  };

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (scanner instanceof CustomDirectoryScanner) {
      ((CustomDirectoryScanner) scanner).setStartScan(historicalScanFinished && startScanningData);
    }
  }

  public static class CustomDirectoryScanner extends DirectoryScanner
  {
    private static final long serialVersionUID = 201508070221L;
    private boolean startScan;

    public CustomDirectoryScanner()
    {
      this.startScan = false;
    }

    public boolean isStartScan()
    {
      return startScan;
    }

    public void setStartScan(boolean startScan)
    {
      this.startScan = startScan;
    }

    @Override
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      if (!isStartScan()) {
        return Sets.newLinkedHashSet();
      }
      return super.scan(fs, filePath, consumedFiles);
    }
  }
}
