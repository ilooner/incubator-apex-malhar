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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

import com.datatorrent.demos.linearroad.data.Pair;
import com.datatorrent.demos.linearroad.data.PartitioningKey;

public class ThroughPutBasedPartitioner implements StatsListener, Partitioner<AccidentNotifier>, Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(ThroughPutBasedPartitioner.class);
  private static final long serialVersionUID = 201508251522L;

  protected transient Map<Integer, Long> throughPutPerOperator;
  private long cooldownMillis = 2000;
  private long nextMillis;
  private long partitionNextMillis;
  private boolean repartition;

  @Min(1)
  private int initialPartitionCount = 1;
  private int partitionCount;
  @Min(1)
  private int minPartitionCount;
  @Min(1)
  private int maxPartitionCount;

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
  }

  /**
   * This creates a partitioner which begins with only one partition.
   */
  public ThroughPutBasedPartitioner()
  {
    throughPutPerOperator = Maps.newHashMap();
  }

  /**
   * This constructor is used to create the partitioner from a property.
   *
   * @param value A string which is an integer of the number of partitions to begin with
   */
  public ThroughPutBasedPartitioner(String value)
  {
    this(Integer.parseInt(value));
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   *
   * @param initialPartitionCount The number of partitions to begin with.
   */
  public ThroughPutBasedPartitioner(int initialPartitionCount)
  {
    this.initialPartitionCount = initialPartitionCount;
    throughPutPerOperator = Maps.newHashMap();
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response response = new Response();
    response.repartitionRequired = false;
    if (throughPutPerOperator == null) {
      throughPutPerOperator = Maps.newHashMap();
    }
    throughPutPerOperator.put(stats.getOperatorId(), stats.getTuplesProcessedPSMA());
    long totalThroughput = 0;
    if (throughPutPerOperator.size() == partitionCount) {
      for (Map.Entry<Integer, Long> entry : throughPutPerOperator.entrySet()) {
        totalThroughput += entry.getValue();
      }
      throughPutPerOperator.clear();
    }
    else {
      return response;
    }

    if ((totalThroughput > 0 && partitionCount != maxPartitionCount) || (totalThroughput == 0 && partitionCount != minPartitionCount)) {
      if (repartition && System.currentTimeMillis() > nextMillis) {
        repartition = false;
        response.repartitionRequired = true;
        partitionCount = totalThroughput > 0 ? maxPartitionCount : minPartitionCount;
        logger.debug("setting repartition to true");
      }
      else if (!repartition) {
        repartition = true;
        nextMillis = System.currentTimeMillis() + cooldownMillis;
      }
    }
    else {
      repartition = false;
    }
    return response;
  }

  @Override
  public Collection<Partition<AccidentNotifier>> definePartitions(Collection<Partition<AccidentNotifier>> partitions, PartitioningContext context)
  {
    if (partitions.iterator().next().getStats() == null) {
      // first call
      // trying to give initial stability before sending the repartition call
      partitionNextMillis = System.currentTimeMillis() + 2 * cooldownMillis;
      nextMillis = partitionNextMillis;
      // delegate to create initial list of partitions
      partitionCount = initialPartitionCount;
      return new CustomStatelessPartitioner<AccidentNotifier>(partitionCount).definePartitions(partitions, context);
    }
    else {
      // repartition call                                                                                                                                    `
      logger.debug("repartition call for operator");
      if (System.currentTimeMillis() < partitionNextMillis) {
        return partitions;
      }

      Collection<Partition<AccidentNotifier>> newPartitions = new CustomStatelessPartitioner<AccidentNotifier>(partitionCount).definePartitions(partitions, context);
      List<HashMap<PartitioningKey, Pair>> accidentKeySets = Lists.newLinkedList();
      List<HashMap<Integer, PartitioningKey>> vehicleId2Segments = Lists.newLinkedList();

      for (Partition<AccidentNotifier> p : partitions) {
        accidentKeySets.add(p.getPartitionedInstance().accidentKeySet);
        vehicleId2Segments.add(p.getPartitionedInstance().vehicleId2Segment);
      }

      for (Partition<AccidentNotifier> p : newPartitions) {
        AccidentNotifier notifier = p.getPartitionedInstance();
        PartitionKeys keys = p.getPartitionKeys().get(context.getInputPorts().get(0));
        Set<Integer> partitionKeySet = keys.partitions;
        for (HashMap<PartitioningKey, Pair> accidentKeySet : accidentKeySets) {
          for (Map.Entry<PartitioningKey, Pair> entry : accidentKeySet.entrySet()) {
            if (partitionKeySet.contains(getPartitioningHashCode(entry.getKey().expressWayId, entry.getKey().direction) & keys.mask)) {
              notifier.accidentKeySet.put(entry.getKey(), entry.getValue());
            }
          }
        }
        for (HashMap<Integer, PartitioningKey> vehicle2Segment : vehicleId2Segments) {
          for (Map.Entry<Integer, PartitioningKey> entry : vehicle2Segment.entrySet()) {
            if (partitionKeySet.contains(getPartitioningHashCode(entry.getValue().expressWayId, entry.getValue().direction) & keys.mask)) {
              notifier.vehicleId2Segment.put(entry.getKey(), entry.getValue());
            }
          }
        }
      }
      partitionNextMillis = System.currentTimeMillis() + cooldownMillis;
      return newPartitions;
    }
  }

  private int getPartitioningHashCode(int expressWay, int direction)
  {
    int result = expressWay;
    result = 31 * result + direction;
    return result;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AccidentNotifier>> partitions)
  {
    logger.debug("Partitioned Map: {}", partitions);
  }

  public long getCooldownMillis()
  {
    return cooldownMillis;
  }

  public int getMinPartitionCount()
  {
    return minPartitionCount;
  }

  public void setMinPartitionCount(int minPartitionCount)
  {
    this.minPartitionCount = minPartitionCount;
  }

  public int getMaxPartitionCount()
  {
    return maxPartitionCount;
  }

  public void setMaxPartitionCount(int maxPartitionCount)
  {
    this.maxPartitionCount = maxPartitionCount;
  }

  public void setCooldownMillis(long cooldownMillis)
  {
    this.cooldownMillis = cooldownMillis;
  }
}
