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
package com.datatorrent.demos.linearroad.data;

import com.datatorrent.demos.linearroad.util.Utils;

public class PositionReport extends LinearRoadTuple
{
  // For Kryo
  private PositionReport()
  {
  }

  private int vehicleId;
  private int vehicleSpeed;
  private int expressWayId;
  private int lane;
  private int direction;
  private int segment;
  private int position;
  private int minute;

  public PositionReport(int eventTime, int vehicleId, int vehicleSpeed, int expressWayId, int lane, int direction, int segment, int position)
  {
    super(0, eventTime);
    this.vehicleId = vehicleId;
    this.vehicleSpeed = vehicleSpeed;
    this.expressWayId = expressWayId;
    this.lane = lane;
    this.direction = direction;
    this.segment = segment;
    this.position = position;
    this.minute = Utils.getMinute(eventTime);
  }

  public int getMinute()
  {
    return minute;
  }

  public void setMinute(int minute)
  {
    this.minute = minute;
  }

  public int getVehicleId()
  {
    return vehicleId;
  }

  public int getVehicleSpeed()
  {
    return vehicleSpeed;
  }

  public int getExpressWayId()
  {
    return expressWayId;
  }

  public int getLane()
  {
    return lane;
  }

  public int getDirection()
  {
    return direction;
  }

  public int getSegment()
  {
    return segment;
  }

  public int getPosition()
  {
    return position;
  }

  @Override
  public String toString()
  {
    return "PositionReport{" +
      "vehicleId=" + vehicleId +
      ", vehicleSpeed=" + vehicleSpeed +
      ", expressWayId=" + expressWayId +
      ", lane=" + lane +
      ", direction=" + direction +
      ", segment=" + segment +
      ", position=" + position +
      ", minute=" + minute +
      '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PositionReport)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    PositionReport report = (PositionReport)o;

    if (vehicleId != report.vehicleId) {
      return false;
    }
    if (vehicleSpeed != report.vehicleSpeed) {
      return false;
    }
    if (expressWayId != report.expressWayId) {
      return false;
    }
    if (lane != report.lane) {
      return false;
    }
    if (direction != report.direction) {
      return false;
    }
    if (segment != report.segment) {
      return false;
    }
    if (position != report.position) {
      return false;
    }
    return minute == report.minute;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + vehicleId;
    result = 31 * result + vehicleSpeed;
    result = 31 * result + expressWayId;
    result = 31 * result + lane;
    result = 31 * result + direction;
    result = 31 * result + segment;
    result = 31 * result + position;
    result = 31 * result + minute;
    return result;
  }
}
