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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.linearroad.data.AverageSpeedTuple;
import com.datatorrent.demos.linearroad.data.Pair;
import com.datatorrent.demos.linearroad.data.PartitioningKey;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.demos.linearroad.data.XwayDirPartitionKey;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class AverageSpeedCalculator extends BaseOperator
{
  protected HashMap<XwayDirPartitionKey, MutableInt> xwayDirPartitionKeyMutableIntHashMap = Maps.newHashMap();
  protected HashMap<XwayDirPartitionKey, HashMap<PartitioningKey, HashMap<Integer, Pair>>> cache = Maps.newHashMap();
  protected transient PartitioningKey partitioningKey;
  protected transient XwayDirPartitionKey xwayDirPartitionKey;
  protected transient MutableInt currentMin;
  public final transient DefaultOutputPort<AverageSpeedTuple> averageSpeed = new DefaultOutputPort<AverageSpeedTuple>();
  public final transient DefaultInputPort<PositionReport> positionReport = new DefaultInputPort<PositionReport>()
  {
    @Override
    public void process(PositionReport positionReport)
    {
      processTuple(positionReport);
    }

    @Override
    public AverageSpeedCodec getStreamCodec()
    {
      return new AverageSpeedCodec();
    }
  };

  protected void processTuple(PositionReport positionReport)
  {
    xwayDirPartitionKey.drainKey(positionReport);
    currentMin = xwayDirPartitionKeyMutableIntHashMap.get(xwayDirPartitionKey);
    if (currentMin != null && currentMin.intValue() != positionReport.getMinute()) {
      if (cache.containsKey(xwayDirPartitionKey)) {
        for (Map.Entry<PartitioningKey, HashMap<Integer, Pair>> entry : cache.remove(xwayDirPartitionKey).entrySet()) {
          PartitioningKey partitioningKey = entry.getKey();
          int totalCars = entry.getValue().size();
          double totalSpeed = 0;
          for (Map.Entry<Integer, Pair> pairEntry : entry.getValue().entrySet()) {
            totalSpeed += ((double)pairEntry.getValue().right / pairEntry.getValue().left);
          }
          AverageSpeedTuple averageSpeedTuple = new AverageSpeedTuple(partitioningKey.expressWayId, partitioningKey.direction, partitioningKey.segment, totalCars, totalSpeed, currentMin.intValue(), totalCars);
          //logger.info(" average speed tuple {}", averageSpeedTuple);
          averageSpeed.emit(averageSpeedTuple);
        }
      }
      currentMin.setValue(positionReport.getMinute());
    }

    if (currentMin == null) {
      xwayDirPartitionKeyMutableIntHashMap.put(xwayDirPartitionKey, currentMin = new MutableInt(-1));
      currentMin.setValue(positionReport.getMinute());
    }
    partitioningKey.drainKey(positionReport);
    HashMap<PartitioningKey, HashMap<Integer, Pair>> xwayDirCache = cache.get(xwayDirPartitionKey);
    if (xwayDirCache == null) {
      cache.put(new XwayDirPartitionKey(positionReport), xwayDirCache = Maps.newHashMap());
    }
    HashMap<Integer, Pair> averageSpeedPerCar = xwayDirCache.get(partitioningKey);
    if (averageSpeedPerCar == null) {
      xwayDirCache.put(new PartitioningKey(positionReport), averageSpeedPerCar = Maps.newHashMap());
    }
    Pair pair = averageSpeedPerCar.get(positionReport.getVehicleId());
    if (pair == null) {
      averageSpeedPerCar.put(positionReport.getVehicleId(), pair = new Pair());
    }
    pair.left++;
    pair.right += positionReport.getVehicleSpeed();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    partitioningKey = new PartitioningKey(-1, 0, 0);
    xwayDirPartitionKey = new XwayDirPartitionKey(-1, 0);
  }

  public static class AverageSpeedCodec extends KryoSerializableStreamCodec<PositionReport>
  {

    @Override
    public int getPartition(PositionReport positionReport)
    {
      int result = positionReport.getExpressWayId();
      result = 31 * result + positionReport.getSegment();
      result = 31 * result + positionReport.getDirection();
      return result;
    }
  }

  private static Logger logger = LoggerFactory.getLogger(AverageSpeedCalculator.class);
}
