/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.state.managed;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.Context;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.OperatorAnnotation;
import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.TimeSlicedBucketedState;
import com.datatorrent.netlet.util.Slice;

/**
 * This implementation of {@link AbstractManagedStateImpl} lets the client to specify the time for each key.
 * The value of time is used to derive the time-bucket of a key.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class ManagedTimeStateImpl<T extends ManagedState.PartitionableManagedStateUser> extends AbstractManagedStateImpl implements
    TimeSlicedBucketedState,
    ManagedState.PartitionableManagedState<T>
{
  @Valid
  @NotNull
  private BucketPartitionManager bucketPartitionManager = new BucketPartitionManager.DefaultBucketPartitionManager();

  public ManagedTimeStateImpl()
  {
    this.numBuckets = 1;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    bucketPartitionManager.initialize(this.numBuckets);
  }

  @Override
  public void put(long bucketId, long time, @NotNull Slice key, @NotNull Slice value)
  {
    bucketPartitionManager.validateBucket(bucketId);
    long timeBucket = timeBucketAssigner.getTimeBucketFor(time);
    putInBucket(bucketId, timeBucket, key, value);
  }

  @Override
  public Slice getSync(long bucketId, @NotNull Slice key)
  {
    bucketPartitionManager.validateBucket(bucketId);
    return getValueFromBucketSync(bucketId, -1, key);
  }

  @Override
  public Slice getSync(long bucketId, long time, @NotNull Slice key)
  {
    bucketPartitionManager.validateBucket(bucketId);
    long timeBucket = timeBucketAssigner.getTimeBucketFor(time);
    if (timeBucket == -1) {
      //time is expired so no point in looking further.
      return BucketedState.EXPIRED;
    }
    return getValueFromBucketSync(bucketId, timeBucket, key);
  }

  @Override
  public Future<Slice> getAsync(long bucketId, Slice key)
  {
    bucketPartitionManager.validateBucket(bucketId);
    return getValueFromBucketAsync(bucketId, -1, key);
  }

  @Override
  public Future<Slice> getAsync(long bucketId, long time, Slice key)
  {
    bucketPartitionManager.validateBucket(bucketId);
    long timeBucket = timeBucketAssigner.getTimeBucketFor(time);
    if (timeBucket == -1) {
      //time is expired so no point in looking further.
      return Futures.immediateFuture(BucketedState.EXPIRED);
    }
    return getValueFromBucketAsync(bucketId, timeBucket, key);
  }

  @Override
  public void clearBucketData()
  {
    this.buckets = null;
  }

  @Override
  public BucketPartitionManager getBucketPartitionManager()
  {
    return bucketPartitionManager;
  }

  @Override
  public void setBucketPartitionManager(@NotNull BucketPartitionManager bucketPartitionManager)
  {
    this.bucketPartitionManager = bucketPartitionManager;
  }

  @Min(1)
  @Override
  public int getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * Sets the number of buckets.
   *
   * @param numBuckets number of buckets
   */
  @Override
  public void setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
  }

  @Override
  public List<Partitioner.Partition<T>> partition(List<T> repartitionedOperators,
      Collection<Partitioner.Partition<T>> originalOperators, Partitioner.PartitioningContext context)
  {
    return bucketPartitionManager.partition(repartitionedOperators,
        originalOperators,
        context);
  }
}
