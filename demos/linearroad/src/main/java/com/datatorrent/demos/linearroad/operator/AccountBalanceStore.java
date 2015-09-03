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

import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.datatorrent.demos.linearroad.data.AccountBalanceQuery;
import com.datatorrent.demos.linearroad.data.QueryResult;
import com.datatorrent.demos.linearroad.data.TollTuple;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;

public class AccountBalanceStore extends AbstractSinglePortHDHTWriter<TollTuple>
{
  private transient List<AccountBalanceQuery> queryList = new ArrayList<>();

  @Override
  public void setup(Context.OperatorContext a)
  {
    super.setup(a);
  }

  @Override
  public Collection<Partition<AbstractSinglePortHDHTWriter<TollTuple>>> definePartitions(Collection<Partition<AbstractSinglePortHDHTWriter<TollTuple>>> a, PartitioningContext a2)
  {
    Collection<Partition<AbstractSinglePortHDHTWriter<TollTuple>>> partitions = super.definePartitions(a, a2);
    DefaultPartition.assignPartitionKeys(partitions, accountBalanceQuery);
    DefaultPartition.assignPartitionKeys(partitions, currentToll);
    return partitions;
  }

  @Override
  protected void processEvent(TollTuple a) throws IOException
  {
    byte[] key = codec.getKeyBytes(a);
    Slice slice = new Slice(key);
    long bucketKey = getBucketKey(a);
    byte[] value = getUncommitted(bucketKey, slice);
    if (value == null) {
      value = get(bucketKey, slice);
    }

    if (value != null) {
      TollTuple temp = codec.fromKeyValue(slice, value);
      a.setTolls(a.getTolls() + temp.getTolls());
    }

    put(bucketKey, slice, codec.getValueBytes(a));
  }

  public transient final DefaultInputPort<TollTuple> currentToll = new DefaultInputPort<TollTuple>()
  {
    @Override
    public void process(TollTuple tollTuple)
    {
      try {
        if (tollTuple.getTolls() != -1) {
          processEvent(tollTuple);
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public AccountBalanceCodec getStreamCodec()
    {
      return new AccountBalanceCodec();
    }
  };

  public transient DefaultOutputPort<QueryResult> accountBalanceQueryResult = new DefaultOutputPort<QueryResult>();

  private long getQueryBucket(AccountBalanceQuery query)
  {
    return query.getVehicleId() & partitionMask;
  }

  public transient final DefaultInputPort<AccountBalanceQuery> accountBalanceQuery = new DefaultInputPort<AccountBalanceQuery>()
  {
    @Override
    public void process(AccountBalanceQuery accountBalanceQuery)
    {
      queryList.add(accountBalanceQuery);
    }

    @Override
    public StreamCodec<AccountBalanceQuery> getStreamCodec()
    {
      return new AccountBalanceQueryCodec();
    }
  };

  @Override
  public void endWindow()
  {
    try {
      for (AccountBalanceQuery accountBalanceQuery : queryList) {
        Slice slice = new Slice(getKeyForQuery(accountBalanceQuery));
        long bucketKey = getQueryBucket(accountBalanceQuery);
        byte[] value = getUncommitted(bucketKey, slice);
        if (value == null) {
          value = get(bucketKey, slice);
        }
        if (value != null) {
          TollTuple tuple = codec.fromKeyValue(slice, value);
          accountBalanceQueryResult.emit(new QueryResult(2, 0, accountBalanceQuery.getEventTime(), accountBalanceQuery.getEventTime() + (System.currentTimeMillis() - accountBalanceQuery.getEntryTime()) / 1000, accountBalanceQuery.getQueryId(), tuple.getTolls(), tuple.getEventTime()));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    queryList.clear();
    super.endWindow();
  }

  private static byte[] getKey(int vehicleId) throws IOException
  {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(outputStream)) {
      oos.writeInt(vehicleId);
      oos.flush();
      return outputStream.toByteArray();
    }
  }

  private static int getParition(int vehicleId)
  {
    return vehicleId;
  }

  private byte[] getKeyForQuery(AccountBalanceQuery query) throws IOException
  {
    return AccountBalanceStore.getKey(query.getVehicleId());
  }

  @Override
  protected HDHTCodec<TollTuple> getCodec()
  {
    return new AccountBalanceCodec();
  }

  public static class AccountBalanceQueryCodec extends KryoSerializableStreamCodec<AccountBalanceQuery>
  {
    @Override
    public int getPartition(AccountBalanceQuery query)
    {
      return AccountBalanceStore.getParition(query.getVehicleId());
    }
  }

  public static class AccountBalanceCodec extends KryoSerializableStreamCodec<TollTuple> implements HDHTCodec<TollTuple>
  {

    @Override
    public int getPartition(TollTuple tollTuple)
    {
      return AccountBalanceStore.getParition(tollTuple.getVehicleId());
    }

    @Override
    public byte[] getKeyBytes(TollTuple tollTuple)
    {
      try {
        return AccountBalanceStore.getKey(tollTuple.getVehicleId());
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
        inputStream.close();
        inputStream = new ByteArrayInputStream(valBytes);
        ois = new ObjectInputStream(inputStream);
        int eventTime = ois.readInt();
        int tolls = ois.readInt();
        return new TollTuple(vehicleId, 0, 0, tolls, eventTime);
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
        oos.writeInt(tollTuple.getEventTime());
        oos.writeInt(tollTuple.getTolls());
        oos.flush();
        return outputStream.toByteArray();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
