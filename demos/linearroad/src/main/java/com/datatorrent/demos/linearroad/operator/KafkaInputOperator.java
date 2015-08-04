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
package com.datatorrent.demos.linearroad.operator;

import java.nio.ByteBuffer;

import kafka.message.Message;

import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.contrib.kafka.AbstractKafkaInputOperator;
import com.datatorrent.contrib.kafka.KafkaConsumer;
import com.datatorrent.demos.linearroad.data.AccountBalanceQuery;
import com.datatorrent.demos.linearroad.data.DailyBalanceQuery;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.netlet.util.DTThrowable;

public class KafkaInputOperator extends AbstractKafkaInputOperator<KafkaConsumer>
{
  public final transient DefaultOutputPort<PositionReport> positionReport = new DefaultOutputPort<PositionReport>();
  public final transient DefaultOutputPort<DailyBalanceQuery> dailyBalanceQuery = new DefaultOutputPort<DailyBalanceQuery>();
  public final transient DefaultOutputPort<AccountBalanceQuery> accountBalanceQuery = new DefaultOutputPort<AccountBalanceQuery>();

  private String delimiter = ",";

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  @Override
  protected void emitTuple(Message message)
  {
    String line;
    try {
      ByteBuffer buffer = message.payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      line = new String(bytes).trim();
    } catch (Exception ex) {
      throw DTThrowable.wrapIfChecked(ex);
    }

    String[] splits = line.split(delimiter);
    int type = Integer.parseInt(splits[0]);
    if (type == 0) {
      positionReport.emit(new PositionReport(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]),
        Integer.parseInt(splits[4]), Integer.parseInt(splits[5]), Integer.parseInt(splits[6]), Integer.parseInt(splits[7]), Integer.parseInt(splits[8])));
    }
    if (type == 2) {
      accountBalanceQuery.emit(new AccountBalanceQuery(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[9])));
    }
    if (type == 3) {
      dailyBalanceQuery.emit(new DailyBalanceQuery(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[9]), Integer.parseInt(splits[4]), Integer.parseInt(splits[14])));
    }
  }
}
