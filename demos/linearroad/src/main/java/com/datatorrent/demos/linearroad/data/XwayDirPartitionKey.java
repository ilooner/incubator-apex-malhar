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

public class XwayDirPartitionKey
{
  public int expressWayId;
  public int direction;

  private XwayDirPartitionKey()
  {

  }

  public XwayDirPartitionKey(int expressWayId, int direction)
  {
    this.expressWayId = expressWayId;
    this.direction = direction;
  }

  public XwayDirPartitionKey(PositionReport positionReport)
  {
    this(positionReport.getExpressWayId(), positionReport.getDirection());
  }

  public void drainKey(PositionReport positionReport)
  {
    expressWayId = positionReport.getExpressWayId();
    direction = positionReport.getDirection();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof XwayDirPartitionKey)) {
      return false;
    }

    XwayDirPartitionKey that = (XwayDirPartitionKey)o;

    if (expressWayId != that.expressWayId) {
      return false;
    }
    return direction == that.direction;

  }

  @Override
  public int hashCode()
  {
    int result = expressWayId;
    result = 31 * result + direction;
    return result;
  }

  @Override
  public String toString()
  {
    return "XwayDirPartitionKey{" +
      "expressWayId=" + expressWayId +
      ", direction=" + direction +
      '}';
  }
}
