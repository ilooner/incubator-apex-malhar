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

public class VehicleSpeedPair
{
  public int totalCars;
  public double totalSpeed;
  public int uniqueCars;

  public VehicleSpeedPair()
  {
    this(0, 0, 0);
  }

  public VehicleSpeedPair(int totalCars, double totalSpeed, int uniqueCars)
  {
    this.totalCars = totalCars;
    this.uniqueCars = uniqueCars;
    this.totalSpeed = totalSpeed;
  }

  @Override
  public String toString()
  {
    return "Pair{" +
      "totalCars=" + totalCars +
      ", uniqueCars=" + uniqueCars +
      ", totalSpeed=" + totalSpeed +
      '}';
  }
}
