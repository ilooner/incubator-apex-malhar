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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;

public class CustomStatelessPartitioner<T> implements Partitioner<T>, Serializable
{

  private static final long serialVersionUID = 201508121710L;

  /**
   * The number of partitions for the default partitioner to create.
   */
  @Min(1)
  private int partitionCount = 1;

  /**
   * This creates a partitioner which creates only one partition.
   */
  public CustomStatelessPartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   *
   * @param value A string which is an integer of the number of partitions to create
   */
  public CustomStatelessPartitioner(String value)
  {
    this(Integer.parseInt(value));
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   *
   * @param partitionCount The number of partitions to create.
   */
  public CustomStatelessPartitioner(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * This method sets the number of partitions for the StatelessPartitioner to create.
   *
   * @param partitionCount The number of partitions to create.
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * This method gets the number of partitions for the StatelessPartitioner to create.
   *
   * @return The number of partitions to create.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> collection, PartitioningContext partitioningContext)
  {
    int parallelPartitionCount = partitioningContext.getParallelPartitionCount();
    int count = (parallelPartitionCount != 0) ? parallelPartitionCount : partitionCount;
    DefaultPartition<T> partition = (DefaultPartition<T>) collection.iterator().next();
    List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
    for (int i = 0; i < count; ++i) {
      DefaultPartition<T> newPartition = new DefaultPartition<T>(partition.getPartitionedInstance());
      newPartitions.add(newPartition);
    }
    // Distribute tuples between partitions for all ports
    for (Operator.InputPort inputPort : partitioningContext.getInputPorts()) {
      DefaultPartition.assignPartitionKeys(newPartitions, inputPort);
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<T>> map)
  {

  }
}
