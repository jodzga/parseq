package com.linkedin.parseq.monitoring;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.monitoring.PlanExecutionMonitor.PlanExecuted;
import com.linkedin.parseq.monitoring.ResourceUtilizationMonitor.UtilizationSnapshot;

public class Monitoring {

  private static final String SELF_APP_PLAN_CLASS = "__self_app_pseudo_plan_class__";

  private final ScheduledExecutorService _executor = Executors.newSingleThreadScheduledExecutor();
  private final ResourceUtilizationMonitor _resourceUtilizationMonitor = new ResourceUtilizationMonitor();
  private final PlanExecutionMonitor _planExecutionMonitor = new PlanExecutionMonitor();

  private UtilizationSnapshot _lastSnapshot = null;

  private void update() {
    UtilizationSnapshot snapshot = _resourceUtilizationMonitor.update();

    if (_lastSnapshot == null) {
      System.out.println("systemLoadAverage,systemCPULoad,processCPULoad,timestamp,ticksTime,totalGcDuration,bytesCollected,"
          + "threadAllocations,threadCPUs,processCPU,youngGCTime,oldGCTime,appTime,safepointTime,vmTime");
    } else {
      System.out.println(snapshot.getSystemLoadAverage() + "," +
          snapshot.getSystemCPULoad() + "," +
          snapshot.getProcessCPULoad() + "," +
          (snapshot.getTimestamp() - _lastSnapshot.getTimestamp()) + "," +
          (snapshot.getTicksTime() - _lastSnapshot.getTicksTime()) + "," +
          (snapshot.getTotalGcDuration() - _lastSnapshot.getTotalGcDuration()) + "," +
          (snapshot.getBytesCollected() - _lastSnapshot.getBytesCollected()) + "," +
          diff(snapshot.getThreadAllocations(), snapshot.getThreadIds(),
              _lastSnapshot.getThreadAllocations(), _lastSnapshot.getThreadIds()) + "," +
          diff(snapshot.getThreadCPUs(), snapshot.getThreadIds(),
              _lastSnapshot.getThreadCPUs(), _lastSnapshot.getThreadIds()) + "," +
          (snapshot.getProcessCPU() - _lastSnapshot.getProcessCPU()) + "," +
          (snapshot.getYoungGCTime() - _lastSnapshot.getYoungGCTime()) + "," +
          (snapshot.getOldGCTime() - _lastSnapshot.getOldGCTime()) + "," +
          (snapshot.getAppTime() - _lastSnapshot.getAppTime()) + "," +
          (snapshot.getSafepointTime() - _lastSnapshot.getSafepointTime()) + "," +
          (snapshot.getVmTime() - _lastSnapshot.getVmTime()));
    }

    //TODO if resources were overloaded in last time slice then ignore executed plans metrics for this slice and the next one too
    // in other words change state to "don't use stats to calculate averages".

    List<PlanExecuted> executedPlans = _planExecutionMonitor.getExecutedPlans();
    //add pseudo plan class which represents all work that is unrelated to execution of plans
    executedPlans.add(new PlanExecuted(SELF_APP_PLAN_CLASS, 1.0));

    if (_planExecutionMonitor.isWarm()) {
      System.out.println(executedPlans);
    }

    _lastSnapshot = snapshot;
  }

  private long diff(long[] metric, long[] threads, long[] lastMetric, long[] lastThreads) {
    Map<Long, Integer> prevIndex = new HashMap<>();
    for (int i = 0; i < lastThreads.length; i++) {
        long id = lastThreads[i];
        prevIndex.put(id, i);
    }
    long value = 0;
    for (int i = 0; i < threads.length; i++) {
        long id = threads[i];
        value += metric[i];
        Integer prev = prevIndex.get(id);
        if (prev != null) {
            value -= lastMetric[prev];
        }
    }
    return value;
  }

  public void planStarted(String planClass) {
    _planExecutionMonitor.planStarted(planClass);
  }

  public void planCompleted(String planClass) {
    _planExecutionMonitor.planCompleted(planClass);
  }

  public void start() {
    _executor.scheduleWithFixedDelay(this::update, 1, 1, TimeUnit.SECONDS);
  }

  public void stop() {
    _executor.shutdown();
  }

}
