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
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.linearroad.data.*;
import com.datatorrent.demos.linearroad.util.Utils;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class TollNotifier extends BaseOperator
{
  private transient TollNotifierKey tollNotifierKey;
  private transient PartitioningKey partitioningKey;
  private final transient List<PositionReport> tupleList = Lists.newLinkedList();

  /*
   * This stores total vehicles, total speed information per minute bucket
   */
  HashMap<TollNotifierKey, VehicleSpeedPair> vehiclesStats = Maps.newHashMap();
  /*
   * This stores the LAV , toll for a given key TODO: Clean it
   */
  HashMap<TollNotifierKey, Pair> lavAndToll = Maps.newHashMap();
  /*
   *This stores vehicleId => (segment id, average speed, toll, event time)
   */
  HashMap<Integer, Triplet> vehicle2SegmentCache = Maps.newHashMap();

  /*
   * Accident (t, x, p, d)
   */
  transient HashSet<TollNotifierKey> accidentKeySet = Sets.newHashSet();

  public final transient DefaultOutputPort<TollTuple> tollCharged = new DefaultOutputPort<TollTuple>();
  public final transient DefaultOutputPort<TollNotificationTuple> tollNotification = new DefaultOutputPort<TollNotificationTuple>();

  public final transient DefaultInputPort<PositionReport> positionReport = new DefaultInputPort<PositionReport>()
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

  private void processPositionReport(PositionReport tuple)
  {
    if (Utils.isExitLane(tuple)) {
      return;
    }
    partitioningKey.drainKey(tuple);
    int vehicleId = tuple.getVehicleId();
    if (vehicle2SegmentCache.containsKey(vehicleId)) {
      if (vehicle2SegmentCache.get(vehicleId).key.equals(partitioningKey)) {
        return;
      }
      //Adding special check to charge the vehicle only if it crosses from one segment into the next one
      PartitioningKey key = vehicle2SegmentCache.get(vehicleId).key;
      if (key.expressWayId == tuple.getExpressWayId() && key.direction == tuple.getDirection()) {
        if (key.direction == 0) {
          if (key.segment != tuple.getSegment() - 1) {
            vehicle2SegmentCache.remove(vehicleId);
          }
        }
        else {
          if (key.segment != tuple.getSegment() + 1) {
            vehicle2SegmentCache.remove(vehicleId);
          }
        }
      }
      else {
        vehicle2SegmentCache.remove(vehicleId);
      }
    }

    Triplet vehicleStats = vehicle2SegmentCache.get(vehicleId);
    if (vehicleStats != null) {
      if (vehicleStats.toll > 0) {
        tollCharged.emit(new TollTuple(vehicleId, 0, tuple.getExpressWayId(), vehicleStats.toll, tuple.getEventTime()));
      }
      vehicleStats.key = new PartitioningKey(tuple);
      vehicleStats.eventTime = tuple.getEventTime();
    }
    else {
      vehicleStats = new Triplet(new PartitioningKey(tuple), 0, 0, tuple.getEventTime());
      vehicle2SegmentCache.put(vehicleId, vehicleStats);
    }

    tollNotifierKey.drainInputTuple(tuple);
    Pair speedTollPair = lavAndToll.get(tollNotifierKey);
    if (speedTollPair == null) {
      boolean reportedAccident = isReportedAccident(tollNotifierKey);
      VehicleSpeedPair vehicle2SpeedPair;
      double averageSpeed = 0;
      int vehiclesInPrevMin = 0;
      //boolean foundLast = false;
      int totalKeysFound = 0;
      for (int i = 1; i <= 5; i++) {
        tollNotifierKey.minute--;
        if (vehiclesStats.containsKey(tollNotifierKey)) {
          totalKeysFound++;
          vehicle2SpeedPair = vehiclesStats.get(tollNotifierKey);
          averageSpeed += (vehicle2SpeedPair.totalSpeed / vehicle2SpeedPair.totalCars);
          if (i == 1) {
            vehiclesInPrevMin = vehicle2SpeedPair.totalCars;
            // foundLast = true;
          }
        }
      }

      //toll and speed calculation
      int toll = 0;
      if (totalKeysFound > 0) {
        averageSpeed = averageSpeed / totalKeysFound;
      }
      else {
        averageSpeed = 0;
      }

      if (averageSpeed < 40 && vehiclesInPrevMin > 50 && !reportedAccident) {
        toll = 2 * (vehiclesInPrevMin - 50) * (vehiclesInPrevMin - 50);
      }
      speedTollPair = new Pair((int) averageSpeed, toll);
      //if (foundLast) {
      lavAndToll.put(new TollNotifierKey(tuple), speedTollPair);
      //remove minute - 5
      vehiclesStats.remove(tollNotifierKey);
      //}
    }
    vehicleStats.averageSpeed = speedTollPair.left;
    vehicleStats.toll = speedTollPair.right;

    tollNotification.emit(new TollNotificationTuple(vehicleId, tuple.getEventTime(), tuple.getEventTime() + (System.currentTimeMillis() - tuple.getEntryTime()) / 1000, vehicleStats.averageSpeed, vehicleStats.toll));
  }

  private boolean isReportedAccident(TollNotifierKey key)
  {
    if (accidentKeySet.contains(key)) {
      return true;
    }
    return false;
  }

  public final transient DefaultInputPort<AverageSpeedTuple> averageSpeedTuple = new DefaultInputPort<AverageSpeedTuple>()
  {
    @Override
    public void process(AverageSpeedTuple averageSpeedTuple)
    {
      vehiclesStats.put(new TollNotifierKey(averageSpeedTuple), new VehicleSpeedPair(averageSpeedTuple.getTotalVehicles(), averageSpeedTuple.getTotalSpeed()));
    }

    @Override
    public StreamCodec<AverageSpeedTuple> getStreamCodec()
    {
      return new AverageSpeedCode();
    }
  };

  public final transient DefaultInputPort<TollNotifierKey> accidentNotification = new DefaultInputPort<TollNotifierKey>()
  {
    @Override
    public void process(TollNotifierKey accidentTuple)
    {
      accidentKeySet.add(accidentTuple);
    }

    @Override
    public StreamCodec<TollNotifierKey> getStreamCodec()
    {
      return new TollNotifierKeyCodec();
    }
  };

  public static class TollNotifierKeyCodec extends KryoSerializableStreamCodec<TollNotifierKey>
  {
    @Override
    public int getPartition(TollNotifierKey tuple)
    {
      int result = tuple.expressWayId;
      result = 31 * result + tuple.direction;
      return result;
    }
  }

  public static class AverageSpeedCode extends KryoSerializableStreamCodec<AverageSpeedTuple>
  {
    @Override
    public int getPartition(AverageSpeedTuple averageSpeedTuple)
    {
      int result = averageSpeedTuple.getExpressWayId();
      result = 31 * result + averageSpeedTuple.getDirection();
      return result;
    }
  }

  public static class TollNotifierKey
  {
    public int expressWayId;
    public int direction;
    public int segment;
    public int minute;

    public TollNotifierKey()
    {

    }

    public TollNotifierKey(AverageSpeedTuple tuple)
    {
      expressWayId = tuple.getExpressWayId();
      direction = tuple.getDirection();
      segment = tuple.getSegment();
      minute = tuple.getMinute();
    }

    public TollNotifierKey(PositionReport tuple)
    {
      expressWayId = tuple.getExpressWayId();
      direction = tuple.getDirection();
      segment = tuple.getSegment();
      minute = tuple.getMinute();
    }

    public void drainKey(AverageSpeedTuple tuple)
    {
      expressWayId = tuple.getExpressWayId();
      direction = tuple.getDirection();
      segment = tuple.getSegment();
      minute = tuple.getMinute();
    }

    public void drainInputTuple(PositionReport tuple)
    {
      expressWayId = tuple.getExpressWayId();
      direction = tuple.getDirection();
      segment = tuple.getSegment();
      minute = tuple.getMinute();
    }

    @Override
    public String toString()
    {
      return "Key{" +
        "expressWayId=" + expressWayId +
        ", direction=" + direction +
        ", segment=" + segment +
        ", minute=" + minute +
        '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TollNotifierKey)) {
        return false;
      }

      TollNotifierKey tollNotifierKey = (TollNotifierKey) o;

      if (expressWayId != tollNotifierKey.expressWayId) {
        return false;
      }
      if (direction != tollNotifierKey.direction) {
        return false;
      }
      if (segment != tollNotifierKey.segment) {
        return false;
      }
      return minute == tollNotifierKey.minute;

    }

    @Override
    public int hashCode()
    {
      int result = expressWayId;
      result = 31 * result + direction;
      result = 31 * result + segment;
      result = 31 * result + minute;
      return result;
    }
  }

  private static class Triplet
  {
    PartitioningKey key;
    int averageSpeed;
    int toll;
    int eventTime;

    private Triplet()
    {

    }

    public Triplet(PartitioningKey key, int averageSpeed, int toll, int eventTime)
    {
      this.key = key;
      this.averageSpeed = averageSpeed;
      this.toll = toll;
      this.eventTime = eventTime;
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    tollNotifierKey = new TollNotifierKey();
    partitioningKey = new PartitioningKey(-1, 0, 0);
  }

  @Override
  public void endWindow()
  {
    for (PositionReport positionReport : tupleList) {
      processPositionReport(positionReport);
    }
    tupleList.clear();
  }

  private static Logger logger = LoggerFactory.getLogger(TollNotifier.class);
}
