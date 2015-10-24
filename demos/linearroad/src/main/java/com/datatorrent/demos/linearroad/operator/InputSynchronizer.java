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

import java.util.Random;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class InputSynchronizer extends BaseOperator
{
  private boolean historicalScanFinished;
  private boolean startScanningData = false;
  private int lowerRangeLimit = 10;
  private int upperRangeLimit = 30;
  private int nextTime = 0;

  public final transient DefaultOutputPort<Integer> nextEventTime = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<Integer> dummyPort = new DefaultOutputPort<>();
  public final transient DefaultInputPort<Boolean> startScanning = new DefaultInputPort<Boolean>()
  {
    @Override
    public void process(Boolean aBoolean)
    {
      historicalScanFinished = aBoolean;
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (historicalScanFinished && startScanningData) {
      nextTime--;
      if (nextTime == -1) {
        int diff = upperRangeLimit - lowerRangeLimit + 1;
        Random random = new Random(System.currentTimeMillis());
        nextTime = lowerRangeLimit + random.nextInt(diff);
        System.out.println(nextTime);
        nextEventTime.emit(nextTime / 2);
      }
    }
  }

  public boolean isHistoricalScanFinished()
  {
    return historicalScanFinished;
  }

  public void setHistoricalScanFinished(boolean historicalScanFinished)
  {
    this.historicalScanFinished = historicalScanFinished;
  }

  public boolean isStartScanningData()
  {
    return startScanningData;
  }

  public void setStartScanningData(boolean startScanningData)
  {
    this.startScanningData = startScanningData;
  }

  public int getLowerRangeLimit()
  {
    return lowerRangeLimit;
  }

  public void setLowerRangeLimit(int lowerRangeLimit)
  {
    this.lowerRangeLimit = lowerRangeLimit;
  }

  public int getUpperRangeLimit()
  {
    return upperRangeLimit;
  }

  public void setUpperRangeLimit(int upperRangeLimit)
  {
    this.upperRangeLimit = upperRangeLimit;
  }
}
