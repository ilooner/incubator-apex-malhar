package org.apache.apex.malhar.lib.state.managed;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.lib.util.KryoCloneUtils;

public class ManagedStateTest
{
  @Test
  public void isPowerOf2Test1()
  {
    Assert.assertTrue(ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.isPowerOf2(1));
  }

  @Test
  public void isPowerOf2Test2()
  {
    Assert.assertTrue(ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.isPowerOf2(2));
  }

  @Test
  public void isPowerOf2Test3()
  {
    Assert.assertFalse(ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.isPowerOf2(3));
  }

  @Test
  public void isPowerOf2Test1024()
  {
    Assert.assertTrue(ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.isPowerOf2(1024));
  }

  @Test
  public void isPowerOf2Test1068()
  {
    Assert.assertFalse(ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.isPowerOf2(1068));
  }

  @Test
  public void log2Test1()
  {
    Assert.assertEquals(0, ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.log2(1));
  }

  @Test
  public void log2Test2()
  {
    Assert.assertEquals(1, ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.log2(2));
  }

  @Test
  public void log2Test1024()
  {
    Assert.assertEquals(10, ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.log2(1024));
  }

  @Test
  public void roundUpToNearestPowerOf2Test6()
  {
    Assert.assertEquals(8,
        ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.roundUpToNearestPowerOf2(6));
  }

  @Test
  public void roundUpToNearestPowerOf2Test9()
  {
    Assert.assertEquals(16,
        ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.roundUpToNearestPowerOf2(9));
  }

  @Test
  public void roundUpToNearestPowerOf2Test1055()
  {
    Assert.assertEquals(2048,
        ManagedState.BucketPartitionManager.DefaultBucketPartitionManager.roundUpToNearestPowerOf2(1055));
  }

  @Test
  public void createPartitionKeysSimpleTest()
  {
    Set<Integer> expectedPartitionKeys = Sets.newHashSet(3, 8, 13);
    Set<Integer> computedPartitionKeys = ManagedState.BucketPartitionManager.DefaultBucketPartitionManager
        .createPartitionKeys(3, 5, 16);

    Assert.assertEquals(expectedPartitionKeys, computedPartitionKeys);
  }

  @Test
  public void initialPartitioningTest()
  {
    MockPartitionableManagedStateUser msu = new MockPartitionableManagedStateUser();
    msu.setNumPartitions(4);
    msu.setNumBuckets(9);

    MockInputPort inputPort1 = new MockInputPort();
    MockInputPort inputPort2 = new MockInputPort();

    List<MockInputPort> inputPorts = Lists.newArrayList(inputPort1, inputPort2);

    MockPartitioningContext partitioningContext = new MockPartitioningContext(0, inputPort1, inputPort2);

    Collection<Partitioner.Partition<MockPartitionableManagedStateUser>> initialPartitions = Lists.<Partitioner
        .Partition<MockPartitionableManagedStateUser>>newArrayList(
        new DefaultPartition(msu));

    Collection<Partitioner.Partition<MockPartitionableManagedStateUser>> repartitioned = msu.definePartitions(
        initialPartitions, partitioningContext);

    List<Set<Long>> buckets = Lists.newArrayList();
    buckets.add(Sets.newHashSet(0L, 1L, 2L));
    buckets.add(Sets.newHashSet(3L, 4L));
    buckets.add(Sets.newHashSet(5L, 6L));
    buckets.add(Sets.newHashSet(7L, 8L));

    List<Partitioner.PartitionKeys> partitionKeys = Lists.newArrayList();
    partitionKeys.add(new Partitioner.PartitionKeys(0x0F, Sets.newHashSet(0, 9, 1, 10, 2, 11)));
    partitionKeys.add(new Partitioner.PartitionKeys(0x0F, Sets.newHashSet(2, 12, 4, 13)));
    partitionKeys.add(new Partitioner.PartitionKeys(0x0F, Sets.newHashSet(5, 14, 6, 15)));
    partitionKeys.add(new Partitioner.PartitionKeys(0x0F, Sets.newHashSet(7, 8)));

    checkPartitioningResult(inputPorts,
        buckets,
        partitionKeys,
        repartitioned,
        4);
  }

  private void checkPartitioningResult(List<MockInputPort> inputPorts,
      List<Set<Long>> buckets,
      List<Partitioner.PartitionKeys> partitionKeys,
      Collection<Partitioner.Partition<MockPartitionableManagedStateUser>> partitions,
      int size)
  {
    Assert.assertEquals(size, partitions.size());
    int partitionCounter = 0;

    for (Partitioner.Partition<MockPartitionableManagedStateUser> partition: partitions) {
      Set<Long> pBuckets = buckets.get(partitionCounter);
      Partitioner.PartitionKeys pPartitionKeys = partitionKeys.get(partitionCounter);

      for (MockInputPort inputPort: inputPorts) {
        Assert.assertEquals(pPartitionKeys, partition.getPartitionKeys().get(inputPort));
      }

      Assert.assertEquals(buckets,
          partition.getPartitionedInstance().getPartitionableManagedState().getBucketPartitionManager().getBuckets());

      partitionCounter++;
    }
  }

  @Test
  public void initialParallelPartitioningTest()
  {
    MockPartitionableManagedStateUser msu = new MockPartitionableManagedStateUser();
    msu.setNumPartitions(5);
    msu.setNumBuckets(7);

    MockInputPort inputPort1 = new MockInputPort();
    MockInputPort inputPort2 = new MockInputPort();

    MockPartitioningContext partitioningContext = new MockPartitioningContext(4, inputPort1, inputPort2);
  }

  public static class MockPartitionableManagedStateUser implements
      ManagedState.PartitionableManagedStateUser<MockPartitionableManagedStateUser>,
      Partitioner<MockPartitionableManagedStateUser>
  {
    private int numPartitions;
    private int numBuckets;
    private ManagedState.PartitionableManagedState partitionableManagedState = new MockPartitionableManagedState();

    public void setNumPartitions(int numPartitions)
    {
      this.numPartitions = numPartitions;
    }

    @Override
    public ManagedState.PartitionableManagedState getPartitionableManagedState()
    {
      return partitionableManagedState;
    }

    @Override
    public void setPartitionableManagedState(ManagedState.PartitionableManagedState partitionableManagedState)
    {
      this.partitionableManagedState = partitionableManagedState;
    }

    @Override
    public int getNumBuckets()
    {
      return numBuckets;
    }

    @Override
    public void setNumBuckets(int numBuckets)
    {
      this.numBuckets = numBuckets;
    }

    @Override
    public Collection<Partition<MockPartitionableManagedStateUser>> definePartitions(
        Collection<Partition<MockPartitionableManagedStateUser>> collection,
        PartitioningContext partitioningContext)
    {
      int totalPartitions = numPartitions;

      if (partitioningContext.getParallelPartitionCount() > 0) {
        totalPartitions = partitioningContext.getParallelPartitionCount();
      }

      Kryo kryo = new Kryo();
      MockPartitionableManagedStateUser mockPartitionableManagedStateUser = collection.iterator().next()
          .getPartitionedInstance();
      List<MockPartitionableManagedStateUser> repartitioned = Lists.newArrayList();

      for (int partitionCount = 0; partitionCount < totalPartitions; partitionCount++) {
        MockPartitionableManagedStateUser cloned = KryoCloneUtils.cloneObject(kryo,
            mockPartitionableManagedStateUser);
        repartitioned.add(cloned);
      }

      return partitionableManagedState.getBucketPartitionManager().partition(repartitioned, collection,
          partitioningContext);
    }

    @Override
    public void partitioned(Map<Integer, Partition<MockPartitionableManagedStateUser>> map)
    {
    }
  }

  public static class MockPartitionableManagedState implements ManagedState.PartitionableManagedState {
    private ManagedState.BucketPartitionManager bucketPartitionManager =
        new ManagedState.BucketPartitionManager.DefaultBucketPartitionManager();
    private int numBuckets;

    @Override
    public ManagedState.BucketPartitionManager getBucketPartitionManager()
    {
      return bucketPartitionManager;
    }

    @Override
    public void setBucketPartitionManager(@NotNull ManagedState.BucketPartitionManager bucketPartitionManager)
    {
      this.bucketPartitionManager = bucketPartitionManager;
    }

    @Override
    public int getNumBuckets()
    {
      return numBuckets;
    }

    @Override
    public void setNumBuckets(@Min(1) int numBuckets)
    {
      this.numBuckets = numBuckets;
    }

    @Override
    public void clearBucketData()
    {
    }

    @Override
    public List<Partitioner.Partition> partition(List repartitionedOperators, Collection originalOperators,
        Partitioner.PartitioningContext context)
    {
      return bucketPartitionManager.partition(repartitionedOperators,
          originalOperators,
          context);
    }
  }

  public static class MockPartitioningContext implements Partitioner.PartitioningContext
  {
    private List<Operator.InputPort<?>> inputPorts;
    private int parallelPartitionCount = 0;

    public MockPartitioningContext(List<Operator.InputPort<?>> inputPorts)
    {
      this.inputPorts = inputPorts;
    }

    public MockPartitioningContext(Operator.InputPort<?>... inputPorts)
    {
      this.inputPorts = Lists.newArrayList();

      for (Operator.InputPort<?> inputPort: inputPorts) {
        this.inputPorts.add(inputPort);
      }
    }

    public MockPartitioningContext(int parallelPartitionCount, List<Operator.InputPort<?>> inputPorts)
    {
      this(inputPorts);
      this.parallelPartitionCount = parallelPartitionCount;
    }

    public MockPartitioningContext(int parallelPartitionCount, Operator.InputPort<?>... inputPorts)
    {
      this(inputPorts);
      this.parallelPartitionCount = parallelPartitionCount;
    }

    @Override
    public int getParallelPartitionCount()
    {
      return parallelPartitionCount;
    }

    @Override
    public List<Operator.InputPort<?>> getInputPorts()
    {
      return inputPorts;
    }
  }

  public static class MockInputPort extends DefaultInputPort
  {
    @Override
    public void process(Object o)
    {
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ManagedStateTest.class);
}
