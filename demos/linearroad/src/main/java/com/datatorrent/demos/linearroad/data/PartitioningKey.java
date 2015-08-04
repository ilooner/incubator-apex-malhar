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

public class PartitioningKey
{
  public int expressWayId;
  public int direction;
  public int segment;

  private PartitioningKey()
  {
  }

  public PartitioningKey(int expressWayId, int direction, int segment)
  {
    this.expressWayId = expressWayId;
    this.direction = direction;
    this.segment = segment;
  }

  public PartitioningKey(PositionReport tuple)
  {
    expressWayId = tuple.getExpressWayId();
    direction = tuple.getDirection();
    segment = tuple.getSegment();
  }

  public void drainKey(PositionReport tuple)
  {
    expressWayId = tuple.getExpressWayId();
    direction = tuple.getDirection();
    segment = tuple.getSegment();
  }

  @Override
  public String toString()
  {
    return "PartitioningKey{" +
      "expressWayId=" + expressWayId +
      ", direction=" + direction +
      ", segment=" + segment +
      '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitioningKey)) {
      return false;
    }

    PartitioningKey that = (PartitioningKey) o;

    if (expressWayId != that.expressWayId) {
      return false;
    }
    if (direction != that.direction) {
      return false;
    }
    return segment == that.segment;

  }

  @Override
  public int hashCode()
  {
    int result = expressWayId;
    result = 31 * result + direction;
    result = 31 * result + segment;
    return result;
  }
}