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

public class AccidentDetectTuple
{
  public int eventTime;
  public int expressWayId;
  public int lane;
  public int direction;
  public int position;

  private AccidentDetectTuple()
  {
  }

  public AccidentDetectTuple(int eventTime, int expressWayId, int lane, int direction, int position)
  {
    this.eventTime = eventTime;
    this.expressWayId = expressWayId;
    this.lane = lane;
    this.direction = direction;
    this.position = position;
  }

  @Override
  public String toString()
  {
    return "AccidentDetectTuple{" +
      "eventTime=" + eventTime +
      ", expressWayId=" + expressWayId +
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
    if (!(o instanceof AccidentDetectTuple)) {
      return false;
    }

    AccidentDetectTuple that = (AccidentDetectTuple) o;

    if (eventTime != that.eventTime) {
      return false;
    }
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
    int result = eventTime;
    result = 31 * result + expressWayId;
    result = 31 * result + lane;
    result = 31 * result + direction;
    result = 31 * result + position;
    return result;
  }
}
