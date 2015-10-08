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
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.linearroad.data.AccidentNotificationTuple;
import com.datatorrent.demos.linearroad.data.Pair;
import com.datatorrent.demos.linearroad.data.PartitioningKey;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.demos.linearroad.util.Utils;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class AccidentNotifier extends BaseOperator
{
  HashMap<PartitioningKey, Pair> accidentKeySet = Maps.newHashMap();
  HashMap<Integer, PartitioningKey> vehicleId2Segment = Maps.newHashMap();
  public final transient DefaultOutputPort<TollNotifier.TollNotifierKey> notifyTollCalculator = new DefaultOutputPort<TollNotifier.TollNotifierKey>();
  public final transient DefaultOutputPort<AccidentNotificationTuple> accidentNotification = new DefaultOutputPort<AccidentNotificationTuple>();

  private final transient List<PositionReport> tupleList = Lists.newLinkedList();
  private transient int MAX_VAL;

  @Override
  public void endWindow()
  {
    super.endWindow();

    for (PositionReport tuple : tupleList) {
      processTuple(tuple);
    }
    tupleList.clear();
  }

  private void processTuple(PositionReport tuple)
  {
    if (Utils.isExitLane(tuple)) {
      return;
    }
    partitioningKey.drainKey(tuple);
    if (vehicleId2Segment.containsKey(tuple.getVehicleId())) {
      PartitioningKey vehicleKey = vehicleId2Segment.get(tuple.getVehicleId());
      if (vehicleKey.equals(partitioningKey)) {
        return;
      }
      vehicleKey.drainKey(tuple);
    } else {
      vehicleId2Segment.put(tuple.getVehicleId(), new PartitioningKey(tuple));
    }

    int eventMinute = tuple.getMinute();
    int eventSegment = tuple.getSegment();
    for (int i = 0; i < 5; i++) {
      if (tuple.getDirection() == 0) {
        partitioningKey.segment = Math.min(eventSegment + i, 99);
      } else {
        partitioningKey.segment = Math.max(eventSegment - i, 0);
      }
      if (accidentKeySet.containsKey(partitioningKey)) {
        Pair accidentTime = accidentKeySet.get(partitioningKey);
        if (eventMinute >= (accidentTime.left + 1) && (accidentTime.right) >= eventMinute) {
          accidentNotification.emit(new AccidentNotificationTuple(tuple.getEventTime(), tuple.getEventTime() + (System.currentTimeMillis() - tuple.getEntryTime()) / 1000, tuple.getVehicleId(), partitioningKey.segment, partitioningKey.expressWayId, partitioningKey.direction));
          notifyTollCalculator.emit(new TollNotifier.TollNotifierKey(tuple));
          return;
        }
      }
    }
  }

  public transient final DefaultInputPort<PositionReport> positionReport = new DefaultInputPort<PositionReport>()
  {
    @Override
    public void process(PositionReport tuple)
    {
      tupleList.add(tuple);
    }

    @Override
    public GenericPositionReportCodec getStreamCodec()
    {
      return new GenericPositionReportCodec();
    }
  };

  public final transient DefaultInputPort<AccidentDetector.AccidentDetectTuple> accidentDetectedReport = new DefaultInputPort<AccidentDetector.AccidentDetectTuple>()
  {
    @Override
    public void process(AccidentDetector.AccidentDetectTuple tuple)
    {
      PartitioningKey partitioningKey1 = new PartitioningKey(tuple.accidentKey.expressWayId, tuple.accidentKey.direction, tuple.accidentKey.position / 5280);
      if (!accidentKeySet.containsKey(partitioningKey1)) {
        accidentKeySet.put(partitioningKey1, new Pair(Utils.getMinute(tuple.eventTime), MAX_VAL));
      }
    }

    @Override
    public AccidentNotifierCodec getStreamCodec()
    {
      return new AccidentNotifierCodec();
    }
  };

  public final transient DefaultInputPort<AccidentDetector.AccidentDetectTuple> accidentClearReport = new DefaultInputPort<AccidentDetector.AccidentDetectTuple>()
  {
    @Override
    public void process(AccidentDetector.AccidentDetectTuple tuple)
    {
      PartitioningKey key = new PartitioningKey(tuple.accidentKey.expressWayId, tuple.accidentKey.direction, tuple.accidentKey.position / 5280);
      if (!accidentKeySet.containsKey(key)) {
        System.out.println("accident key set " + accidentKeySet + "invalid key " + positionReport);
        throw new RuntimeException("Invalid clear accident report" + key);
      }
      int minute = Utils.getMinute(tuple.eventTime);
      if (minute < accidentKeySet.get(key).right) {
        accidentKeySet.get(key).right = minute;
      }
    }

    @Override
    public AccidentNotifierCodec getStreamCodec()
    {
      return new AccidentNotifierCodec();
    }
  };

  private transient PartitioningKey partitioningKey;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    partitioningKey = new PartitioningKey(-1, 0, 0);
    MAX_VAL = Integer.MAX_VALUE - 1;
  }

  public static class AccidentNotifierCodec extends KryoSerializableStreamCodec<AccidentDetector.AccidentDetectTuple>
  {
    @Override
    public int getPartition(AccidentDetector.AccidentDetectTuple accidentDetectTuple)
    {
      int result = accidentDetectTuple.accidentKey.expressWayId;
      result = 31 * result + accidentDetectTuple.accidentKey.direction;
      return result;
    }
  }
}
