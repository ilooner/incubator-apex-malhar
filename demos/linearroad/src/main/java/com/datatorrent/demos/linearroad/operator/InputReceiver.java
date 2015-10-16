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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.demos.linearroad.data.AccountBalanceQuery;
import com.datatorrent.demos.linearroad.data.DailyBalanceQuery;
import com.datatorrent.demos.linearroad.data.LinearRoadTuple;
import com.datatorrent.demos.linearroad.data.PositionReport;

public class InputReceiver implements InputOperator, Partitioner<InputReceiver>, Operator.IdleTimeHandler
{
  private transient FileSystem fs;
  private transient Path filePath;
  private transient BufferedReader br;
  private transient MutableInt offset;
  private transient MutableInt skipOffset;
  private transient LinearRoadTuple linearRoadTuple;
  private boolean historicalScanFinished;
  private boolean startScanningData = false;
  private transient List<BufferedReader> bufferedReaders;
  private List<String> filesToScan;
  @NotNull
  private String delimiter = ",";
  @NotNull
  private String directory;
  private int numberOfPartitions = Integer.MAX_VALUE;
  private int emitBatchSize = 1000;
  private List<MutableInt> offsets = Lists.newArrayList();
  private transient List<MutableInt> skipOffsets = Lists.newArrayList();
  private boolean emit;
  private boolean emitAllBool = false;
  private boolean ignoreHeader = false;

  public boolean isIgnoreHeader()
  {
    return ignoreHeader;
  }

  public void setIgnoreHeader(boolean ignoreHeader)
  {
    this.ignoreHeader = ignoreHeader;
  }

  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }

  public int getNumberOfPartitions()
  {
    return numberOfPartitions;
  }

  public void setNumberOfPartitions(int numberOfPartitions)
  {
    this.numberOfPartitions = numberOfPartitions;
  }

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

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

  public final transient DefaultOutputPort<Boolean> emitAll = new DefaultOutputPort<Boolean>();

  public boolean isHistoricalScanFinished()
  {
    return historicalScanFinished;
  }

  public void setHistoricalScanFinished(boolean historicalScanFinished)
  {
    this.historicalScanFinished = historicalScanFinished;
  }

  protected void openFile(Path path)
  {
    try {
      InputStream is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));
      bufferedReaders.add(br);
      if (ignoreHeader) {
        br.readLine(); // to ignore header
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

  }

  protected void closeFile(BufferedReader bufferedReader) throws IOException
  {
    bufferedReader.close();
  }

  @Override
  public void handleIdleTime()
  {
    emitTuples();
  }

  @Override
  public void emitTuples()
  {
    if (!emit || bufferedReaders == null || bufferedReaders.isEmpty()) {
      return;
    }
    emit = false;
    Iterator<BufferedReader> bufferedReaderIterator = bufferedReaders.iterator();
    Iterator<MutableInt> offsetIterator = offsets.iterator();
    Iterator<MutableInt> skipIterator = skipOffsets.iterator();
    Iterator<String> fileIterator = filesToScan.iterator();

    while (bufferedReaderIterator.hasNext()) {
      br = bufferedReaderIterator.next();
      offset = offsetIterator.next();
      skipOffset = skipIterator.next();
      fileIterator.next();
      int localBatchSize = 0;
      try {
        while (localBatchSize < emitBatchSize) {
          linearRoadTuple = readEntity();
          if (linearRoadTuple == null) {
            closeFile(br);
            bufferedReaderIterator.remove();
            offsetIterator.remove();
            skipIterator.remove();
            fileIterator.remove();
            break;
          }
          if (skipOffset.intValue() > 0) {
            skipOffset.decrement();
          } else {
            offset.increment();
            emit(linearRoadTuple);
          }
          localBatchSize++;
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

    }
  }

  protected LinearRoadTuple readEntity() throws IOException
  {
    String line = br.readLine();
    if (line != null) {
      String[] splits = line.split(delimiter);
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

  protected void emit(LinearRoadTuple tuple)
  {
    if (tuple instanceof PositionReport) {
      positionReport.emit((PositionReport)tuple);
    } else if (tuple instanceof DailyBalanceQuery) {
      dailyBalanceQuery.emit((DailyBalanceQuery)tuple);
    } else if (tuple instanceof AccountBalanceQuery) {
      accountBalanceQuery.emit((AccountBalanceQuery)tuple);
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
    readFiles();
    if (filesToScan.isEmpty() && !emitAllBool) {
      emitAll.emit(true);
      emitAllBool = true;
    }
  }

  private void readFiles()
  {
    if (filesToScan != null && !filesToScan.isEmpty() && historicalScanFinished && startScanningData && bufferedReaders.isEmpty()) {
      for (String file : filesToScan) {
        openFile(new Path(file));
      }
      if (!offsets.isEmpty()) {
        assert filesToScan.size() == offsets.size() : "Offset size can't be different than files to scan";
        for (MutableInt offset : offsets) {
          skipOffsets.add(new MutableInt(offset.intValue()));
        }
      } else {
        for (String file : filesToScan) {
          offsets.add(new MutableInt(0));
          skipOffsets.add(new MutableInt(0));
        }
      }
    }
  }

  @Override
  public Collection<Partition<InputReceiver>> definePartitions(Collection<Partition<InputReceiver>> partitions, PartitioningContext context)
  {
    try {
      Path dir = new Path(directory);
      FileSystem fileSystem = FileSystem.newInstance(dir.toUri(), new Configuration());
      FileStatus[] fileStatus = fileSystem.listStatus(dir);
      numberOfPartitions = Math.min(fileStatus.length, numberOfPartitions);
      List<Partition<InputReceiver>> newPartitions = Lists.newArrayListWithExpectedSize(numberOfPartitions);
      Kryo kryo = new Kryo();
      // Kryo.copy fails as it attempts to clone transient fields
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Output loutput = new Output(bos);
      kryo.writeObject(loutput, this);
      loutput.close();

      for (int i = 0; i < numberOfPartitions; i++) {
        Input lInput = new Input(bos.toByteArray());
        @SuppressWarnings("unchecked")
        InputReceiver oper = kryo.readObject(lInput, this.getClass());
        lInput.close();
        oper.filesToScan = Lists.newArrayList();
        newPartitions.add(new DefaultPartition<InputReceiver>(oper));
      }
      int i = 0;
      for (FileStatus fileStatus1 : fileStatus) {
        newPartitions.get(i % numberOfPartitions).getPartitionedInstance().filesToScan.add(fileStatus1.getPath().toString());
        i++;
      }
      return newPartitions;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long l)
  {
    emit = true;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      filePath = new Path(directory);
      fs = FileSystem.newInstance(filePath.toUri(), new Configuration());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    bufferedReaders = Lists.newArrayList();
    readFiles();
  }

  @Override
  public void teardown()
  {
    try {
      fs.close();
      for (BufferedReader bufferedReader : bufferedReaders) {
        bufferedReader.close();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<InputReceiver>> map)
  {

  }
}
