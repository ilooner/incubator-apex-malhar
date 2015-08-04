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

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.linearroad.data.AverageSpeedTuple;
import com.datatorrent.demos.linearroad.data.Pair;
import com.datatorrent.demos.linearroad.data.PartitioningKey;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class AverageSpeedCalculator extends BaseOperator
{
  private int currentMinute = -1;
  private HashMap<PartitioningKey, HashMap<Integer, Pair>> cache = Maps.newHashMap();
  private transient PartitioningKey partitioningKey;
  public final transient DefaultOutputPort<AverageSpeedTuple> averageSpeed = new DefaultOutputPort<AverageSpeedTuple>();
  public final transient DefaultInputPort<PositionReport> positionReport = new DefaultInputPort<PositionReport>()
  {
    @Override
    public void process(PositionReport positionReport)
    {
      if (currentMinute != positionReport.getMinute()) {
        for (Map.Entry<PartitioningKey, HashMap<Integer, Pair>> entry : cache.entrySet()) {
          PartitioningKey partitioningKey = entry.getKey();
          int totalCars = entry.getValue().size();
          double totalSpeed = 0;
          for (Map.Entry<Integer, Pair> pairEntry : entry.getValue().entrySet()) {
            totalSpeed += ((double) pairEntry.getValue().right / pairEntry.getValue().left);
          }
          AverageSpeedTuple averageSpeedTuple = new AverageSpeedTuple(partitioningKey.expressWayId, partitioningKey.direction, partitioningKey.segment, totalCars, totalSpeed, currentMinute);
          //logger.info(" average speed tuple {}", averageSpeedTuple);
          averageSpeed.emit(averageSpeedTuple);
        }
        currentMinute = positionReport.getMinute();
        cache.clear();
      }
      partitioningKey.drainKey(positionReport);
      HashMap<Integer, Pair> averageSpeedPerCar = cache.get(partitioningKey);
      if (averageSpeedPerCar == null) {
        averageSpeedPerCar = Maps.newHashMap();
        cache.put(new PartitioningKey(positionReport), averageSpeedPerCar);
      }
      Pair pair = averageSpeedPerCar.get(positionReport.getVehicleId());
      if (pair == null) {
        pair = new Pair();
        averageSpeedPerCar.put(positionReport.getVehicleId(), pair);
      }
      pair.left++;
      pair.right += positionReport.getVehicleSpeed();
    }

    @Override
    public AverageSpeedCodec getStreamCodec()
    {
      return new AverageSpeedCodec();
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    partitioningKey = new PartitioningKey(-1, 0, 0);
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
