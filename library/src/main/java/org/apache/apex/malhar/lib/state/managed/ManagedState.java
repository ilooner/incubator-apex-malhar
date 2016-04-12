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

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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

  interface PartitionableManagedStateUser
  {
    public PartitionableManagedState getPartitionableManagedState();
    public void setPartitionableManagedState(PartitionableManagedState partitionableManagedState);
    public int getNumBuckets();
    public void setNumBuckets(int numBuckets);
  }

  interface ManagedStatePartitioner
  {
    public void partition(List<Partitioner.Partition<PartitionableManagedStateUser>> repartitionedOperators,
        List<Partitioner.Partition<PartitionableManagedStateUser>> originalOperators);
  }

  interface PartitionableManagedState extends ManagedStatePartitioner
  {
    public BucketPartitionManager getBucketPartitionManager();
    public void setBucketPartitionManager(BucketPartitionManager bucketPartitionManager);
    @Min(1)
    public int getNumBuckets();
    public void setNumBuckets(@Min(1) int numBuckets);
  }

  interface BucketPartitionManager extends ManagedStatePartitioner
  {
    public Set<Long> getBuckets();
    public void setBuckets(Set<Long> buckets);

    public abstract class AbstractBucketPartitionManager implements BucketPartitionManager
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
    }

    public class DefaultBucketPartitionManager extends AbstractBucketPartitionManager
    {
      @Override
      public void partition(List<Partitioner.Partition<PartitionableManagedStateUser>> repartitionedOperators,
          List<Partitioner.Partition<PartitionableManagedStateUser>> originalOperators)
      {
        int numBuckets = getNumBuckets(originalOperators);
        int numNewPartitions = repartitionedOperators.size();

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

          for (int bucketPartitionCount = 0; bucketPartitionCount < numBucketsPerPartition; bucketPartitionCount++,
              bucketCounter++) {
            buckets.add(bucketCounter);
          }

          if (remainderBuckets == 0) {
            continue;
          }

          bucketCounter++;
          buckets.add(bucketCounter);
        }

        KryoCloneUtils.cloneObject(kryo, )
      }

      private PartitionableManagedState getPartitionableManagedState(
          List<Partitioner.Partition<PartitionableManagedStateUser>> originalOperators)
      {
        return originalOperators.iterator().next().getPartitionedInstance().getPartitionableManagedState();
      }

      private int getNumBuckets(List<Partitioner.Partition<PartitionableManagedStateUser>> originalOperators)
      {
        return originalOperators.iterator().next().getPartitionedInstance().getNumBuckets();
      }
    }
  }
}
