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

public class TollTuple
{
  int vehicleId;
  int day;
  int expressWayId;
  int tolls;
  int eventTime;

  private TollTuple()
  {

  }

  public TollTuple(int vehicleId, int day, int expressWayId, int tolls, int eventTime)
  {
    this.vehicleId = vehicleId;
    this.day = day;
    this.expressWayId = expressWayId;
    this.tolls = tolls;
    this.eventTime = eventTime;
  }

  @Override
  public String toString()
  {
    return "TollHistoryTuple{" +
      "vehicleId=" + vehicleId +
      ", day=" + day +
      ", expressWayId=" + expressWayId +
      ", tolls=" + tolls +
      ", entryTime=" + eventTime +
      '}';
  }

  public void setTolls(int tolls)
  {
    this.tolls = tolls;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TollTuple)) {
      return false;
    }

    TollTuple that = (TollTuple)o;

    if (vehicleId != that.vehicleId) {
      return false;
    }
    if (day != that.day) {
      return false;
    }
    if (expressWayId != that.expressWayId) {
      return false;
    }
    if (tolls != that.tolls) {
      return false;
    }
    return eventTime == that.eventTime;

  }

  @Override
  public int hashCode()
  {
    int result = vehicleId;
    result = 31 * result + day;
    result = 31 * result + expressWayId;
    result = 31 * result + tolls;
    result = 31 * result + eventTime;
    return result;
  }

  public int getVehicleId()
  {
    return vehicleId;
  }

  public int getDay()
  {
    return day;
  }

  public int getExpressWayId()
  {
    return expressWayId;
  }

  public int getTolls()
  {
    return tolls;
  }

  public int getEventTime()
  {
    return eventTime;
  }
}

