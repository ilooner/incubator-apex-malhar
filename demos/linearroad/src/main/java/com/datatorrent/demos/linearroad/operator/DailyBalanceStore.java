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

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.datatorrent.demos.linearroad.data.DailyBalanceQuery;
import com.datatorrent.demos.linearroad.data.QueryResult;
import com.datatorrent.demos.linearroad.data.TollTuple;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

public class DailyBalanceStore extends AbstractSinglePortHDHTWriter<TollTuple>
{

  private transient List<DailyBalanceQuery> queryList = new ArrayList<>();
  private transient DailyBalanceQueryCodec dailyBalanceQueryCodec;

  @Override
  public void setup(Context.OperatorContext a)
  {
    super.setup(a);
    dailyBalanceQueryCodec = new DailyBalanceQueryCodec();
  }

  @Override
  public Collection<Partition<AbstractSinglePortHDHTWriter<TollTuple>>> definePartitions(Collection<Partition<AbstractSinglePortHDHTWriter<TollTuple>>> a, PartitioningContext a2)
  {
    Collection<Partition<AbstractSinglePortHDHTWriter<TollTuple>>> partitions = super.definePartitions(a, a2);
    DefaultPartition.assignPartitionKeys(partitions, dailyBalanceQuery);
    DefaultPartition.assignPartitionKeys(partitions, currentToll);
    return partitions;
  }

  @Override
  protected void processEvent(TollTuple a) throws IOException
  {
    byte[] key = codec.getKeyBytes(a);
    Slice slice = new Slice(key);
    long bucketKey = getBucketKey(a);
    /*  Since I know that the data is not going to be added
    byte[] value = getUncommitted(bucketKey, slice);
    if (value == null) {
      value = get(bucketKey, slice);
    }

    if (value != null) {
      TollTuple temp = codec.fromKeyValue(slice, value);
      a.setTolls(a.getTolls() + temp.getTolls());
    } */
    put(bucketKey, slice, codec.getValueBytes(a));
  }

  @InputPortFieldAnnotation(optional = true)
  public transient final DefaultInputPort<TollTuple> currentToll = new DefaultInputPort<TollTuple>()
  {
    @Override
    public void process(TollTuple tollTuple)
    {
      /*
      try {
        if (tollTuple.getTolls() != -1) {
          processEvent(tollTuple);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } */
    }

    @Override
    public DailyBalanceCodec getStreamCodec()
    {
      return new DailyBalanceCodec();
    }
  };

  public transient final DefaultOutputPort<QueryResult> dailyBalanceQueryResult = new DefaultOutputPort<QueryResult>();

  private long getQueryBucket(DailyBalanceQuery query)
  {
    return dailyBalanceQueryCodec.getPartition(query) & partitionMask;
  }

  public transient final DefaultInputPort<DailyBalanceQuery> dailyBalanceQuery = new DefaultInputPort<DailyBalanceQuery>()
  {
    @Override
    public void process(DailyBalanceQuery dailyBalanceQuery)
    {
      queryList.add(dailyBalanceQuery);
    }

    @Override
    public StreamCodec<DailyBalanceQuery> getStreamCodec()
    {
      return new DailyBalanceQueryCodec();
    }
  };

  @Override
  public void endWindow()
  {
    try {
      for (DailyBalanceQuery dailyBalanceQuery : queryList) {
        if (dailyBalanceQuery.getDay() == 0) {
          continue;
        }
        Slice slice = new Slice(getKeyForQuery(dailyBalanceQuery));
        long bucketKey = getQueryBucket(dailyBalanceQuery);
        byte[] value = getUncommitted(bucketKey, slice);
        if (value == null) {
          value = get(bucketKey, slice);
        }
        if (value != null) {
          TollTuple tuple = codec.fromKeyValue(slice, value);
          dailyBalanceQueryResult.emit(new QueryResult(3, 0, dailyBalanceQuery.getEventTime(), dailyBalanceQuery.getEventTime() + (System.currentTimeMillis() - dailyBalanceQuery.getEntryTime()) / 1000, dailyBalanceQuery.getQueryId(), tuple.getTolls(), tuple.getEventTime()));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    queryList.clear();
    super.endWindow();
  }

  private static int getParition(int expressWayId, int day, int vehicleId)
  {
    int result = vehicleId;
    result = 31 * result + expressWayId;
    result = 31 * result + day;
    return result;
  }

  private static byte[] getKey(int expressWayId, int day, int vehicleId) throws IOException
  {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(outputStream)) {
      oos.writeInt(vehicleId);
      oos.writeInt(expressWayId);
      oos.writeInt(day);
      oos.flush();
      return outputStream.toByteArray();
    }
  }

  public static class DailyBalanceQueryCodec extends KryoSerializableStreamCodec<DailyBalanceQuery>
  {
    @Override
    public int getPartition(DailyBalanceQuery tuple)
    {
      return DailyBalanceStore.getParition(tuple.getExpressWayId(), tuple.getDay(), tuple.getVehicleId());
    }
  }

  private byte[] getKeyForQuery(DailyBalanceQuery tuple) throws IOException
  {
    return DailyBalanceStore.getKey(tuple.getExpressWayId(), tuple.getDay(), tuple.getVehicleId());
  }

  @Override
  protected HDHTCodec<TollTuple> getCodec()
  {
    return new DailyBalanceCodec();
  }

  public static class DailyBalanceCodec extends KryoSerializableStreamCodec<TollTuple> implements AbstractSinglePortHDHTWriter.HDHTCodec<TollTuple>
  {
    @Override
    public int getPartition(TollTuple tuple)
    {
      return DailyBalanceStore.getParition(tuple.getExpressWayId(), tuple.getDay(), tuple.getVehicleId());
    }

    @Override
    public byte[] getKeyBytes(TollTuple tuple)
    {
      try {
        return DailyBalanceStore.getKey(tuple.getExpressWayId(), tuple.getDay(), tuple.getVehicleId());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public TollTuple fromKeyValue(Slice key, byte[] valBytes)
    {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(key.buffer);
      ObjectInputStream ois = null;
      try {
        ois = new ObjectInputStream(inputStream);
        int vehicleId = ois.readInt();
        int expressWayId = ois.readInt();
        int day = ois.readInt();
        inputStream.close();
        inputStream = new ByteArrayInputStream(valBytes);
        ois = new ObjectInputStream(inputStream);
        int tolls = ois.readInt();
        return new TollTuple(vehicleId, day, expressWayId, tolls, 0);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } finally {
        if (ois != null) {
          try {
            ois.close();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    }

    @Override
    public byte[] getValueBytes(TollTuple tollTuple)
    {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(outputStream)) {
        oos.writeInt(tollTuple.getTolls());
        oos.flush();
        return outputStream.toByteArray();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
