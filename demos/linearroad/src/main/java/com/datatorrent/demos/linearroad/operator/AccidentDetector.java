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
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.demos.linearroad.util.Utils;

public class AccidentDetector extends BaseOperator
{
  private HashMap<Integer, MutablePair<PositionReport, MutableInt>> vehicleIdPosition = Maps.newHashMap();
  private HashMap<Integer, MutablePair<AccidentDetectTuple, List<Integer>>> accidentVehicleMap = Maps.newHashMap();
  private HashSet<AccidentKey> accidentKeyHashSet = Sets.newHashSet();
  private HashMap<AccidentKey, HashMap<Integer, Integer>> stoppedVehiclesMap = Maps.newHashMap();

  private static Logger logger = LoggerFactory.getLogger(AccidentDetector.class);

  public final transient DefaultOutputPort<AccidentDetectTuple> accidentDetectedReport = new DefaultOutputPort<AccidentDetectTuple>();
  public final transient DefaultOutputPort<AccidentDetectTuple> accidentClearReport = new DefaultOutputPort<AccidentDetectTuple>();

  public final transient DefaultInputPort<PositionReport> positionReport = new DefaultInputPort<PositionReport>()
  {
    @Override
    public void process(PositionReport positionReport)
    {
      if (!Utils.isTravelLane(positionReport)) {
        clearAccident(positionReport, vehicleIdPosition.get(positionReport.getVehicleId()));
        vehicleIdPosition.remove(positionReport.getVehicleId());
        return;
      }

      if (vehicleIdPosition.containsKey(positionReport.getVehicleId())) {
        MutablePair<PositionReport, MutableInt> existingVehiclePosition = vehicleIdPosition.get(positionReport.getVehicleId());
        if (isVehicleStopped(existingVehiclePosition.getLeft(), positionReport)) {
          existingVehiclePosition.getRight().increment();
          if (existingVehiclePosition.getRight().intValue() >= 4) {
            if (!accidentVehicleMap.containsKey(positionReport.getVehicleId())) {
              AccidentKey accidentKey = new AccidentKey(positionReport);
              //This means that accident has occurred but this car is not involved in accident
              if (!accidentKeyHashSet.contains(accidentKey)) {
                if (stoppedVehiclesMap.containsKey(accidentKey)) {
                  //Possible Accident Detected
                  HashMap<Integer, Integer> stoppedVehicleIds2EventTime = stoppedVehiclesMap.get(accidentKey);
                  stoppedVehicleIds2EventTime.remove(positionReport.getVehicleId());
                  List<Integer> vehiclesInAccident = Lists.newLinkedList();
                  for (Map.Entry<Integer, Integer> entry : stoppedVehicleIds2EventTime.entrySet()) {
                    if (entry.getValue() <= positionReport.getEventTime() && positionReport.getEventTime() <= entry.getValue() + 30) {
                      vehiclesInAccident.add(entry.getKey());
                    }
                  }
                  stoppedVehicleIds2EventTime.put(positionReport.getVehicleId(), positionReport.getEventTime());
                  if (vehiclesInAccident.size() > 0) {
                    vehiclesInAccident.add(positionReport.getVehicleId());
                    //Accident detected
                    MutablePair<AccidentDetectTuple, List<Integer>> accidentKeyListMutablePair = new MutablePair<>(new AccidentDetectTuple(accidentKey, positionReport.getEventTime()), vehiclesInAccident);
                    accidentVehicleMap.put(positionReport.getVehicleId(), accidentKeyListMutablePair);
                    for (Integer entry : vehiclesInAccident) {
                      accidentVehicleMap.put(entry, accidentKeyListMutablePair);
                    }
                    accidentDetectedReport.emit(accidentKeyListMutablePair.getLeft());
                    accidentKeyHashSet.add(accidentKey);
                    logger.info("accident detected {}", accidentKeyListMutablePair.getLeft());
                  }
                }
                else {
                  HashMap<Integer, Integer> vehicleIds2EventTime = Maps.newHashMap();
                  vehicleIds2EventTime.put(positionReport.getVehicleId(), positionReport.getEventTime());
                  stoppedVehiclesMap.put(accidentKey, vehicleIds2EventTime);
                }
              }
            }
          }
          existingVehiclePosition.setLeft(positionReport);
        }
        else {
          clearAccident(positionReport, existingVehiclePosition);
          existingVehiclePosition.setLeft(positionReport);
          existingVehiclePosition.getRight().setValue(1);
        }
      }
      else {
        vehicleIdPosition.put(positionReport.getVehicleId(), new MutablePair<PositionReport, MutableInt>(positionReport, new MutableInt(1)));
      }
    }

    @Override
    public GenericPositionReportCodec getStreamCodec()
    {
      return new GenericPositionReportCodec();
    }
  };

  private void clearAccident(PositionReport positionReport, MutablePair<PositionReport, MutableInt> existingVehiclePosition)
  {
    if (accidentVehicleMap.containsKey(positionReport.getVehicleId())) {
      MutablePair<AccidentDetectTuple, List<Integer>> accidentKeyListMutablePair = accidentVehicleMap.remove(positionReport.getVehicleId());
      //Accident cleared
      HashMap<Integer, Integer> vehicleIds2EventTime = stoppedVehiclesMap.get(accidentKeyListMutablePair.getLeft().accidentKey);
      for (Integer vehicleId : accidentKeyListMutablePair.getRight()) {
        accidentVehicleMap.remove(vehicleId);
        vehicleIds2EventTime.remove(vehicleId);
      }
      if (vehicleIds2EventTime.size() == 0) {
        stoppedVehiclesMap.remove(accidentKeyListMutablePair.getLeft().accidentKey);
      }
      accidentKeyListMutablePair.getLeft().eventTime = positionReport.getEventTime();
      accidentClearReport.emit(accidentKeyListMutablePair.getLeft());
      accidentKeyHashSet.remove(accidentKeyListMutablePair.getLeft().accidentKey);
      logger.info("accident clear {}", accidentKeyListMutablePair.getLeft());
    }
    else if (existingVehiclePosition != null && existingVehiclePosition.getRight().intValue() >= 4) {
      AccidentKey key = new AccidentKey(existingVehiclePosition.getLeft());
      HashMap<Integer, Integer> vehicleIds2EventTime = stoppedVehiclesMap.get(key);
      if (vehicleIds2EventTime != null) {
        vehicleIds2EventTime.remove(positionReport.getVehicleId());
        if (vehicleIds2EventTime.size() == 0) {
          stoppedVehiclesMap.remove(key);
        }
      }
    }
  }

  private boolean isVehicleStopped(PositionReport first, PositionReport second)
  {
    if (first.getDirection() == second.getDirection() && first.getExpressWayId() == second.getExpressWayId() && first.getLane() == second.getLane()
      && first.getPosition() == second.getPosition() && (second.getEventTime() - first.getEventTime() == 30)) {
      return true;
    }
    return false;
  }

  public static class AccidentDetectTuple
  {
    public int eventTime;
    public AccidentKey accidentKey;

    private AccidentDetectTuple()
    {
    }

    public AccidentDetectTuple(AccidentKey accidentKey, int eventTime)
    {
      this.accidentKey = accidentKey;
      this.eventTime = eventTime;
    }

    @Override
    public String toString()
    {
      return "AccidentTuple{" +
        "eventTime=" + Utils.getMinute(eventTime) +
        ", accidentKey=" + accidentKey +
        '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AccidentDetectTuple)) {
        return false;
      }

      AccidentDetectTuple that = (AccidentDetectTuple) o;

      if (eventTime != that.eventTime) {
        return false;
      }
      return !(accidentKey != null ? !accidentKey.equals(that.accidentKey) : that.accidentKey != null);

    }

    @Override
    public int hashCode()
    {
      int result = eventTime;
      result = 31 * result + (accidentKey != null ? accidentKey.hashCode() : 0);
      return result;
    }
  }

  public static class AccidentKey
  {
    public int expressWayId;
    public int lane;
    public int direction;
    public int position;

    private AccidentKey()
    {
    }

    public AccidentKey(PositionReport tuple)
    {
      expressWayId = tuple.getExpressWayId();
      lane = tuple.getLane();
      direction = tuple.getDirection();
      position = tuple.getPosition();
    }

    public void drainKey(PositionReport tuple)
    {
      expressWayId = tuple.getExpressWayId();
      lane = tuple.getLane();
      direction = tuple.getDirection();
      position = tuple.getPosition();
    }

    @Override
    public String toString()
    {
      return "AccidentKey{" +
        "expressWayId=" + expressWayId +
        ", lane=" + lane +
        ", direction=" + direction +
        ", position=" + position +
        '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AccidentKey)) {
        return false;
      }

      AccidentKey that = (AccidentKey) o;

      if (expressWayId != that.expressWayId) {
        return false;
      }
      if (lane != that.lane) {
        return false;
      }
      if (direction != that.direction) {
        return false;
      }

      return position == that.position;

    }

    @Override
    public int hashCode()
    {
      int result = expressWayId;
      result = 31 * result + lane;
      result = 31 * result + direction;
      result = 31 * result + position;
      return result;
    }
  }
}
