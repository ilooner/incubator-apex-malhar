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
package com.datatorrent.demos.linearroad;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.demos.linearroad.data.AccountBalanceQuery;
import com.datatorrent.demos.linearroad.data.DailyBalanceQuery;
import com.datatorrent.demos.linearroad.data.PositionReport;
import com.datatorrent.demos.linearroad.operator.*;

@ApplicationAnnotation(name = "LinearRoad")
public class LinearRoadBenchmark implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    int numberOfExpressWays = configuration.getInt("dt.application.linearroad.numberOfExpressWays", 1);
    boolean enablePartitioning = configuration.getBoolean("dt.application.linearroad.enablePartitioning", true);
    boolean dynamicPartitioning = configuration.getBoolean("dt.application.linearroad.dynamicPartitioning", false);
    boolean isKafka = configuration.getBoolean("dt.application.linearroad.kafka", true);
    HistoricalInputReceiver historicalInputReceiver = dag.addOperator("HistoricalReceiver", new HistoricalInputReceiver());
    DefaultOutputPort<PositionReport> positionReport;
    DefaultOutputPort<DailyBalanceQuery> dailyBalanceQuery;
    DefaultOutputPort<AccountBalanceQuery> accountBalanceQuery;
    if (isKafka) {
      KafkaInputOperator receiver = dag.addOperator("KafkaReceiver", new KafkaInputOperator());
      dag.addStream("start-stream-data", historicalInputReceiver.readCurrentData, receiver.startScanning);
      positionReport = receiver.positionReport;
      dailyBalanceQuery = receiver.dailyBalanceQuery;
      accountBalanceQuery = receiver.accountBalanceQuery;
    }
    else {
      InputReceiver receiver = dag.addOperator("Receiver", new InputReceiver());
      receiver.setScanner(new InputReceiver.CustomDirectoryScanner());
      dag.addStream("start-stream-data", historicalInputReceiver.readCurrentData, receiver.startScanning);
      positionReport = receiver.positionReport;
      dailyBalanceQuery = receiver.dailyBalanceQuery;
      accountBalanceQuery = receiver.accountBalanceQuery;
    }

    AverageSpeedCalculatorV2 averageSpeedCalculator = dag.addOperator("AverageSpeedCalculator", new AverageSpeedCalculatorV2());
    TollNotifier tollNotifier = dag.addOperator("TollNotifier", new TollNotifier());
    AccidentDetector accidentDetector = dag.addOperator("AccidentDetector", new AccidentDetector());
    AccidentNotifier accidentNotifier = dag.addOperator("AccidentNotifier", new AccidentNotifier());
    AccountBalanceStore accountBalanceStore = dag.addOperator("AccountBalanceStore", new AccountBalanceStore());
    DailyBalanceStore dailyBalanceStore = dag.addOperator("DailyBalanceStore", new DailyBalanceStore());
    //setting partitions
    if (enablePartitioning) {
      dag.setAttribute(accidentDetector, Context.OperatorContext.PARTITIONER, new CustomStatelessPartitioner<Operator>(numberOfExpressWays * 2));
      dag.setAttribute(averageSpeedCalculator, Context.OperatorContext.PARTITIONER, new CustomStatelessPartitioner<Operator>(numberOfExpressWays * 2));
      if (dynamicPartitioning) {
        ThroughPutBasedPartitioner throughPutBasedPartitioner = new ThroughPutBasedPartitioner(1);
        throughPutBasedPartitioner.setMinPartitionCount(configuration.getInt("dt.application.linearroad.accidentNotifier.minPartitions", 1));
        throughPutBasedPartitioner.setMaxPartitionCount(configuration.getInt("dt.application.linearroad.accidentNotifier.maxPartitions", 4));
        throughPutBasedPartitioner.setCooldownMillis(configuration.getInt("dt.application.linearroad.accidentNotifier.cooldownMillis", 30000));
        dag.setAttribute(accidentNotifier, Context.OperatorContext.PARTITIONER, throughPutBasedPartitioner);
        dag.setAttribute(accidentNotifier, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{throughPutBasedPartitioner}));
      }
      else {
        dag.setAttribute(accidentNotifier, Context.OperatorContext.PARTITIONER, new CustomStatelessPartitioner<Operator>(numberOfExpressWays * 2));
      }
      dag.setAttribute(tollNotifier, Context.OperatorContext.PARTITIONER, new CustomStatelessPartitioner<Operator>(numberOfExpressWays * 2));
    }

    HdfsOutputOperator accidentNotifierConsole = dag.addOperator("Accident-Notifier-Console", new HdfsOutputOperator());
    HdfsOutputOperator tollNotifierConsole = dag.addOperator("Toll-Notifier-Console", new HdfsOutputOperator());
    HdfsOutputOperator dailyBalanceConsole = dag.addOperator("Daily-Balance-Console", new HdfsOutputOperator());
    HdfsOutputOperator accountBalanceConsole = dag.addOperator("Account-Balance-Console", new HdfsOutputOperator());

    dag.addStream("position-report", positionReport, accidentDetector.positionReport, averageSpeedCalculator.positionReport, accidentNotifier.positionReport, tollNotifier.positionReport);
    dag.addStream("current-toll-balance", tollNotifier.tollCharged, accountBalanceStore.currentToll);

    dag.addStream("historical-toll-balance", historicalInputReceiver.tollHistoryTuplePort, dailyBalanceStore.input, accountBalanceStore.input);
    dag.addStream("daily-balance-query", dailyBalanceQuery, dailyBalanceStore.dailyBalanceQuery);
    dag.addStream("account-balance-query", accountBalanceQuery, accountBalanceStore.accountBalanceQuery);

    dag.addStream("average-speed", averageSpeedCalculator.averageSpeed, tollNotifier.averageSpeedTuple);
    dag.addStream("accident-clear-report", accidentDetector.accidentClearReport, accidentNotifier.accidentClearReport);
    dag.addStream("accident-detect-report", accidentDetector.accidentDetectedReport, accidentNotifier.accidentDetectedReport);
    dag.addStream("accident-notifier", accidentNotifier.accidentNotification, accidentNotifierConsole.input);
    dag.addStream("accident-toll-notifier", accidentNotifier.notifyTollCalculator, tollNotifier.accidentNotification);
    dag.addStream("toll-notifier", tollNotifier.tollNotification, tollNotifierConsole.input);
    dag.addStream("daily-balance-result", dailyBalanceStore.dailyBalanceQueryResult, dailyBalanceConsole.input);
    dag.addStream("account-balance-result", accountBalanceStore.accountBalanceQueryResult, accountBalanceConsole.input);
  }
}
