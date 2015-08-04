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

public class AccidentNotificationTuple
{
  private int type;
  private int eventTime;
  private long emit;
  private int vehicleId;
  private int segment;

  private AccidentNotificationTuple()
  {
    type = 1;
  }

  public AccidentNotificationTuple(int eventTime, long emit, int vehicleId, int segment)
  {
    this.type = 1;
    this.eventTime = eventTime;
    this.emit = emit;
    this.vehicleId = vehicleId;
    this.segment = segment;
  }

  @Override
  public String toString()
  {
    return type +
      "," + eventTime +
      "," + emit +
      "," + vehicleId +
      "," + segment;
  }



  public int getType()
  {
    return type;
  }

  public int getEventTime()
  {
    return eventTime;
  }

  public long getEmit()
  {
    return emit;
  }

  public int getSegment()
  {
    return segment;
  }
}
