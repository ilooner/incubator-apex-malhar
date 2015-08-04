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

public class TollNotificationTuple
{
  int type;
  int vehicleId;
  long entryTime;
  long exitTime;
  int averageSpeed;
  int toll;

  private TollNotificationTuple()
  {

  }

  public TollNotificationTuple(int vehicleId, long entryTime, long exitTime, int averageSpeed, int toll)
  {
    this.type = 0;
    this.vehicleId = vehicleId;
    this.entryTime = entryTime;
    this.exitTime = exitTime;
    this.averageSpeed = averageSpeed;
    this.toll = toll;
  }

  @Override
  public String toString()
  {
    return
      type +
        "," + vehicleId +
        "," + entryTime +
        "," + exitTime +
        "," + averageSpeed +
        "," + toll;
  }
}
