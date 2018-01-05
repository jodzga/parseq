package com.linkedin.parseq.monitoring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class PlanExecutionMonitor {

  /*
   * if sample is taken every second, this amounts to 1 minute
   */
  private static final int MAX_SAMPLES_FOR_AVERAGING = 60;

  private final Map<String, AtomicReference<PlanClassState>> _states = new ConcurrentHashMap<>();
  private final Map<String, int[]> _arrivals = new HashMap<>();
  private final Map<String, long[]> _times = new HashMap<>();
  private final Map<String, Integer> _arrivalsSum = new HashMap<>();
  private final Map<String, Long> _timesSum = new HashMap<>();
  private int _idx = 0;
  private boolean _warm = false;

  public void planStarted(String planClass) {
    update(true, System.nanoTime(), planClass);
  }

  public void planCompleted(String planClass) {
    update(false, System.nanoTime(), planClass);
  }

  /**
   * Returns number of plans executed in last time slice for every plan class.
   * Time slice is defined as a time interval between consecutive invocations of this method.
   * Number of plans executed does not have to be integer.
   * For example if plan class "A" takes on average 2 seconds to execute and this method is executed once per second then
   * one time slice will report that plan class "A" was executed ~0.5 times and one or two adjacent time slices will
   * report that plan "A" was executed < 0.5 times and all counts should sum up to ~1.
   * Time slice is defined as a time interval between consecutive invocations of this method.
   * @return
   */
  public List<PlanExecuted> getExecutedPlans() {
    final long timestamp = System.nanoTime();
    final List<PlanExecuted> slices = new ArrayList<>();

    _states.forEach((planClass, ref) -> {
      PlanClassState state = rollOver(timestamp, planClass);
      if (state != null) {
        slices.add(new PlanExecuted(planClass, executionsInSlice(planClass, state)));
      }
    });

    _idx++;
    if (_idx == MAX_SAMPLES_FOR_AVERAGING) {
      _warm = true;
      _idx = 0;
    }

    return slices;
  }

  public boolean isWarm() {
    return _warm;
  }

  private double executionsInSlice(String planClass, PlanClassState state) {
    int[] arrivals = _arrivals.computeIfAbsent(planClass, p -> new int[MAX_SAMPLES_FOR_AVERAGING]);
    long[] times = _times.computeIfAbsent(planClass, p -> new long[MAX_SAMPLES_FOR_AVERAGING]);

    int arrivalsSum = _arrivalsSum.getOrDefault(planClass, 0) - arrivals[_idx] + state._sliceArrivals;
    long timesSum = _timesSum.getOrDefault(planClass, 0L) - times[_idx] + state._sliceSum;

    _arrivalsSum.put(planClass, arrivalsSum);
    _timesSum.put(planClass, timesSum);

    arrivals[_idx] = state._sliceArrivals;
    times[_idx] = state._sliceSum;

    /*
     * Get avg time using little's law:
     * L = λ*W
     * W = L/λ
     */
    double avgTime = ((double)timesSum) / arrivalsSum;

    /*
     * using little's law: L = λ*W
     * λ = L/W
     */
    return ((double)state._sliceSum) / avgTime;
  }

  private void update(boolean isArrival, long timestamp, String planClass) {
    AtomicReference<PlanClassState> ref = _states.computeIfAbsent(planClass, key -> new AtomicReference<>());
    ref.getAndUpdate(prev -> update(prev, isArrival, timestamp));
  }

  private PlanClassState update(PlanClassState prev, boolean isArrival, long timestamp) {
    int change = isArrival ? 1 : -1;
    if (prev != null) {
      return new PlanClassState(prev._inProgressCount + change,
          prev._sliceStartTime,
          timestamp,
          prev._sliceSum +
            ((timestamp - prev._lastUpdateTime) > 0 ?
                (timestamp - prev._lastUpdateTime) * prev._inProgressCount : 0),
          prev._sliceArrivals + (isArrival ? 1 : 0));
    } else {
      return new PlanClassState(change, timestamp, timestamp, 0, isArrival ? 1 : 0);
    }
  }

  private PlanClassState rollOver(long timestamp, String planClass) {
    AtomicReference<PlanClassState> ref = _states.computeIfAbsent(planClass, key -> new AtomicReference<>());
    PlanClassState prev = null, next = null;
    boolean updated = false;
    while (!updated) {
      prev = ref.get();
      next = new PlanClassState(prev._inProgressCount, timestamp, timestamp, 0, 0);
      updated = ref.compareAndSet(prev, next);
    }
    if (prev == null) {
      return null;
    } else {
      return new PlanClassState(prev._inProgressCount, prev._sliceStartTime, timestamp,
          prev._sliceSum +
          ((timestamp - prev._lastUpdateTime) > 0 ?
              (timestamp - prev._lastUpdateTime) * prev._inProgressCount : 0),
          prev._sliceArrivals);
    }
  }

  private static class PlanClassState {
    private final int _inProgressCount;
    private final long _sliceStartTime;
    private final long _lastUpdateTime;
    private final long _sliceSum;
    private final int _sliceArrivals;
    public PlanClassState(int inProgressCount, long sliceStartTime, long lastUpdateTime, long sliceSum, int sliceArrivals) {
      if (inProgressCount < 0) {
        throw new IllegalArgumentException("inProgressCount < 0");
      }
      if (sliceArrivals < 0) {
        throw new IllegalArgumentException("sliceArrivals < 0");
      }
      if (lastUpdateTime - sliceStartTime < 0) {
        throw new IllegalArgumentException("lastUpdateTime lower than sliceStartTime");
      }
      if (sliceSum < 0) {
        throw new IllegalArgumentException("sliceSum < 0");
      }
      _inProgressCount = inProgressCount;
      _sliceStartTime = sliceStartTime;
      _lastUpdateTime = lastUpdateTime;
      _sliceSum = sliceSum;
      _sliceArrivals = sliceArrivals;
    }
  }

  public static class PlanExecuted {
    private final String _planClass;
    private final Double _count;

    public PlanExecuted(String planClass, Double count) {
      _planClass = planClass;
      _count = count;
    }

    public String getPlanClass() {
      return _planClass;
    }

    public Double getCount() {
      return _count;
    }

    @Override
    public String toString() {
      return "[" + _planClass + "=" + _count + "]";
    }

  }

}
