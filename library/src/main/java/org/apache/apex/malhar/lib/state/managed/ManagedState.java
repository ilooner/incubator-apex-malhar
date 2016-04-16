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
import java.util.Set;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.lib.util.KryoCloneUtils;

/**
 * Managed state has a limit on amount of data in memory.
 */
public interface ManagedState
{
  /**
   * Sets the maximum memory size.
   * @param bytes max size in bytes.
   */
  void setMaxMemorySize(long bytes);

  interface PartitionableManagedStateUser<T extends PartitionableManagedStateUser>
  {
    public PartitionableManagedState<T> getPartitionableManagedState();

    public void setPartitionableManagedState(PartitionableManagedState<T> partitionableManagedState);

    public int getNumBuckets();

    public void setNumBuckets(int numBuckets);
  }

  interface ManagedStatePartitioner<T extends PartitionableManagedStateUser>
  {
    public List<Partitioner.Partition<T>> partition(
        List<T> repartitionedOperators,
        Collection<Partitioner.Partition<T>> originalOperators,
        Partitioner.PartitioningContext context);
  }

  interface PartitionableManagedState<T extends PartitionableManagedStateUser> extends ManagedStatePartitioner<T>
  {
    @NotNull
    public BucketPartitionManager<T> getBucketPartitionManager();

    public void setBucketPartitionManager(@NotNull BucketPartitionManager<T> bucketPartitionManager);

    @Min(1)
    public int getNumBuckets();

    public void setNumBuckets(@Min(1) int numBuckets);

    public void clearBucketData();
  }

  interface BucketPartitionManager<T extends PartitionableManagedStateUser> extends ManagedStatePartitioner<T>
  {
    public void initialize(int numBuckets);

    public Set<Long> getBuckets();

    public void setBuckets(Set<Long> buckets);

    public void validateBucket(long bucket);

    public abstract class AbstractBucketPartitionManager<T extends PartitionableManagedStateUser> implements
        BucketPartitionManager<T>
    {
      private Set<Long> buckets;

      @Override
      public Set<Long> getBuckets()
      {
        return buckets;
      }

      @Override
      public void setBuckets(Set<Long> buckets)
      {
        Preconditions.checkNotNull(buckets);
        this.buckets = buckets;
      }

      @Override
      public void validateBucket(long bucket)
      {
        Preconditions.checkArgument(buckets.contains(bucket));
      }
    }

    public class DefaultBucketPartitionManager<T extends PartitionableManagedStateUser> extends
        AbstractBucketPartitionManager<T>
    {
      private static final int SHIFT_MASK = 0x40000000;

      @Override
      public void initialize(int numBuckets)
      {
        Preconditions.checkArgument(numBuckets == getBuckets().size());

        Set<Long> buckets = Sets.newHashSet();

        for (long bucketCounter = 0; bucketCounter < (long)numBuckets; bucketCounter++) {
          buckets.add(bucketCounter);
        }

        this.setBuckets(buckets);
      }

      @Override
      public List<Partitioner.Partition<T>>
          partition(List<T> repartitionedOperators,
          Collection<Partitioner.Partition<T>> originalOperators,
          Partitioner.PartitioningContext context)
      {
        boolean isParallelPartition = context.getParallelPartitionCount() > 0;

        List<Partitioner.Partition<T>> partitions = Lists.newArrayList();

        int numBuckets = getNumBuckets(originalOperators);
        int numNewPartitions = repartitionedOperators.size();
        int numPartitionKeys = roundUpToNearestPowerOf2(numBuckets);
        LOG.info("numPartitionKeys {}", numPartitionKeys);
        int partitionMask = numPartitionKeys - 1;

        Preconditions.checkArgument(numBuckets > 0);
        Preconditions.checkArgument(numNewPartitions > 0);
        Preconditions.checkArgument(numBuckets >= numNewPartitions);

        PartitionableManagedState partitionableManagedState = getPartitionableManagedState(originalOperators);
        int numBucketsPerPartition = numBuckets / numNewPartitions;
        int remainderBuckets = numBuckets % numNewPartitions;

        Kryo kryo = new Kryo();

        long bucketCounter = 0;

        for (int partitionCount = 0; partitionCount < numNewPartitions; partitionCount++) {
          Set<Long> buckets = Sets.newTreeSet();
          Set<Integer> partitionKeys = Sets.newHashSet();

          for (int bucketPartitionCount = 0; bucketPartitionCount < numBucketsPerPartition; bucketPartitionCount++,
              bucketCounter++) {
            buckets.add(bucketCounter);

            if (!isParallelPartition) {
              partitionKeys.addAll(createPartitionKeys(bucketCounter, numBuckets, numPartitionKeys));
            }
          }

          if (remainderBuckets > 0) {
            remainderBuckets--;
            bucketCounter++;
            buckets.add(bucketCounter);

            if (!isParallelPartition) {
              partitionKeys.addAll(createPartitionKeys(bucketCounter, numBuckets, numPartitionKeys));
            }
          }

          LOG.info("partition {} partitionKeys {} buckets {}", partitionCount, partitionKeys, buckets);

          PartitionableManagedState clonedManagedState = KryoCloneUtils.cloneObject(kryo, partitionableManagedState);
          clonedManagedState.setNumBuckets(buckets.size());
          clonedManagedState.getBucketPartitionManager().setBuckets(buckets);
          clonedManagedState.clearBucketData();

          T managedStateUser = repartitionedOperators.get(partitionCount);
          managedStateUser.setPartitionableManagedState(clonedManagedState);

          DefaultPartition<T> defaultPartition = new DefaultPartition<T>(managedStateUser);

          if (!isParallelPartition) {
            for (Operator.InputPort<?> inputPort : context.getInputPorts()) {
              defaultPartition.getPartitionKeys().put(inputPort, new Partitioner.PartitionKeys(
                  partitionMask, partitionKeys));
            }
          }

          partitions.add(defaultPartition);
        }

        return partitions;
      }

      private PartitionableManagedState getPartitionableManagedState(
          Collection<Partitioner.Partition<T>> originalOperators)
      {
        return originalOperators.iterator().next().getPartitionedInstance().getPartitionableManagedState();
      }

      private int getNumBuckets(Collection<Partitioner.Partition<T>> originalOperators)
      {
        return originalOperators.iterator().next().getPartitionedInstance().getNumBuckets();
      }

      public static int roundUpToNearestPowerOf2(int num)
      {
        Preconditions.checkArgument(num > 0);
        Preconditions.checkArgument((SHIFT_MASK) > num);

        if (isPowerOf2(num)) {
          return num;
        }

        int shiftCounter = 0;

        for (; shiftCounter < 32; shiftCounter++) {
          int mask = SHIFT_MASK >>> shiftCounter;

          if ((mask & num) != 0) {
            break;
          }
        }

        return SHIFT_MASK >>> shiftCounter;
      }

      public static boolean isPowerOf2(int num)
      {
        Preconditions.checkArgument(num > 0);

        return ((~num >> 1) & num) == num;
      }

      public static int log2(int powerOf2)
      {
        Preconditions.checkArgument(powerOf2 > 0);

        int power = 0;

        for (;powerOf2 > 1;power++) {
          powerOf2 >>= 1;
        }

        return power;
      }

      public static Set<Integer> createPartitionKeys(long bucket, int numBuckets, int numPartitionKeys)
      {
        Set<Integer> buckets = Sets.newHashSet();
        int currentKey = (int)bucket;

        for (;currentKey < numPartitionKeys;currentKey += numBuckets) {
          buckets.add(currentKey);
        }

        return buckets;
      }
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(ManagedState.class);
}
