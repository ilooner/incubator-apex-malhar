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

public class SegmentHistoryTuple
{
  int day;
  int minute;
  int expressWayId;
  int direction;
  int segment;
  int lav;
  int totalVehicles;
  int toll;

  private SegmentHistoryTuple()
  {

  }

  public SegmentHistoryTuple(int day, int minute, int expressWayId, int direction, int segment, int lav, int totalVehicles, int toll)
  {
    this.day = day;
    this.minute = minute;
    this.expressWayId = expressWayId;
    this.direction = direction;
    this.segment = segment;
    this.lav = lav;
    this.totalVehicles = totalVehicles;
    this.toll = toll;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentHistoryTuple)) {
      return false;
    }

    SegmentHistoryTuple that = (SegmentHistoryTuple) o;

    if (day != that.day) {
      return false;
    }
    if (minute != that.minute) {
      return false;
    }
    if (expressWayId != that.expressWayId) {
      return false;
    }
    if (direction != that.direction) {
      return false;
    }
    if (segment != that.segment) {
      return false;
    }
    if (lav != that.lav) {
      return false;
    }
    if (totalVehicles != that.totalVehicles) {
      return false;
    }
    return toll == that.toll;

  }

  @Override
  public int hashCode()
  {
    int result = day;
    result = 31 * result + minute;
    result = 31 * result + expressWayId;
    result = 31 * result + direction;
    result = 31 * result + segment;
    result = 31 * result + lav;
    result = 31 * result + totalVehicles;
    result = 31 * result + toll;
    return result;
  }

  @Override
  public String toString()
  {
    return "SegmentHistoryTuple{" +
      "day=" + day +
      ", minute=" + minute +
      ", expressWayId=" + expressWayId +
      ", direction=" + direction +
      ", segment=" + segment +
      ", lav=" + lav +
      ", totalVehicles=" + totalVehicles +
      ", toll=" + toll +
      '}';
  }
}
