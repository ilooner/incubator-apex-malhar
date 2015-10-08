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

public class AccountBalanceQuery extends LinearRoadTuple
{
  int vehicleId;
  int queryId;

  AccountBalanceQuery()
  {

  }

  public AccountBalanceQuery(int eventTime, int vehicleId, int queryId)
  {
    this(2, eventTime, vehicleId, queryId);
  }

  public AccountBalanceQuery(int reportType, int eventTime, int vehicleId, int queryId)
  {
    super(reportType, eventTime);
    this.vehicleId = vehicleId;
    this.queryId = queryId;
  }

  public int getVehicleId()
  {
    return vehicleId;
  }

  public int getQueryId()
  {
    return queryId;
  }

  @Override
  public String toString()
  {
    return "AccountBalanceQuery{" +
      "vehicleId=" + vehicleId +
      ", queryId=" + queryId +
      ", " + super.toString() +
      '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AccountBalanceQuery)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    AccountBalanceQuery that = (AccountBalanceQuery)o;

    if (vehicleId != that.vehicleId) {
      return false;
    }
    return queryId == that.queryId;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + vehicleId;
    result = 31 * result + queryId;
    return result;
  }
}
