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

public class AverageSpeedTuple
{
  private int expressWayId;
  private int direction;
  private int segment;
  private int totalVehicles;
  private double totalSpeed;
  private int minute;

  private AverageSpeedTuple()
  {

  }

  public AverageSpeedTuple(int expressWayId, int direction, int segment, int totalVehicles, double totalSpeed, int minute)
  {
    this.expressWayId = expressWayId;
    this.direction = direction;
    this.segment = segment;
    this.totalVehicles = totalVehicles;
    this.totalSpeed = totalSpeed;
    this.minute = minute;
  }

  public int getExpressWayId()
  {
    return expressWayId;
  }

  public int getDirection()
  {
    return direction;
  }

  public int getSegment()
  {
    return segment;
  }

  public int getTotalVehicles()
  {
    return totalVehicles;
  }

  public double getTotalSpeed()
  {
    return totalSpeed;
  }

  public int getMinute()
  {
    return minute;
  }

  @Override
  public String toString()
  {
    return "AverageSpeedTuple{" +
      "expressWayId=" + expressWayId +
      ", direction=" + direction +
      ", segment=" + segment +
      ", totalVehicles=" + totalVehicles +
      ", totalSpeed=" + totalSpeed +
      ", minute=" + minute +
      '}';
  }
}
