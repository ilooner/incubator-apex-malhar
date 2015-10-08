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

public class QueryResult extends LinearRoadTuple
{
  long exitTime;
  int queryId;
  int balance;
  int resultTime;

  private QueryResult()
  {

  }

  public QueryResult(int type, long entryTime, int eventTime, long exitTime, int queryId, int balance, int resultTime)
  {
    super(type, entryTime, eventTime);
    this.exitTime = exitTime;
    this.queryId = queryId;
    this.balance = balance;
    this.resultTime = resultTime;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    QueryResult that = (QueryResult)o;

    if (exitTime != that.exitTime) {
      return false;
    }
    if (queryId != that.queryId) {
      return false;
    }
    if (balance != that.balance) {
      return false;
    }
    return resultTime == that.resultTime;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (int)(exitTime ^ (exitTime >>> 32));
    result = 31 * result + queryId;
    result = 31 * result + balance;
    result = 31 * result + resultTime;
    return result;
  }

  @Override
  public String toString()
  {
    if (getType() == 2) {
      return
        getType() +
          "," + getEventTime() +
          "," + exitTime +
          "," + resultTime +
          "," + queryId +
          "," + balance;
    } else {
      return getType() +
        "," + getEventTime() +
        "," + exitTime +
        "," + queryId +
        "," + balance;
    }
  }

}
