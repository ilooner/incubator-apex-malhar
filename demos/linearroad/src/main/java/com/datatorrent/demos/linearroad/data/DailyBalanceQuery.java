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

public class DailyBalanceQuery extends AccountBalanceQuery
{
  int expressWayId;
  int day;

  public DailyBalanceQuery()
  {

  }

  public DailyBalanceQuery(int eventTime, int vehicleId, int queryId, int expressWayId, int day)
  {
    this(3, eventTime, vehicleId, queryId, expressWayId, day);
  }

  public DailyBalanceQuery(int reportType, int eventTime, int vehicleId, int queryId, int expressWayId, int day)
  {
    super(reportType, eventTime, vehicleId, queryId);
    this.expressWayId = expressWayId;
    this.day = day;
  }

  public int getExpressWayId()
  {
    return expressWayId;
  }

  public int getDay()
  {
    return day;
  }

  @Override
  public String toString()
  {
    return "DailyBalanceQuery{" +
      "expressWayId=" + expressWayId +
      ", day=" + day +
      ", " + super.toString() +
      '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DailyBalanceQuery)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    DailyBalanceQuery that = (DailyBalanceQuery)o;

    if (expressWayId != that.expressWayId) {
      return false;
    }
    return day == that.day;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + expressWayId;
    result = 31 * result + day;
    return result;
  }
}
