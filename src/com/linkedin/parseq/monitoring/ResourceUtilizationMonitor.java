package com.linkedin.parseq.monitoring;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;

import sun.management.counter.Counter;
import sun.management.counter.perf.PerfInstrumentation;
import sun.misc.Perf;

public class ResourceUtilizationMonitor {

  private final ThreadMXBean _threadBean;
  private final OperatingSystemMXBean _osBean;
  private final PerfInstrumentation _perfInstr;
  private final AtomicLong _totalGcDuration = new AtomicLong(0);
  private final AtomicLong _bytesCollected = new AtomicLong(0);
  private final long _sunOsHrtFrequency;
  private final long _tickToNanoMultiplier;

  public ResourceUtilizationMonitor() {

    // construct PerfInstrumentation object
    Perf perf =  AccessController.doPrivileged(new Perf.GetPerfAction());
    try {
        ByteBuffer bb = perf.attach(0, "r");
        if (bb.capacity() > 0) {
          _perfInstr = new PerfInstrumentation(bb);
        } else {
          throw new RuntimeException("Could not construct obtain PerfInstrumentation. "
              + "Make sure shared memory for perf data is not turned of by -XX:-UsePerfData.");
        }
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Could not construct obtain PerfInstrumentation. "
          + "Make sure shared memory for perf data is not turned of by -XX:-UsePerfData.", e);
    } catch (IOException e) {
      throw new RuntimeException("Could not construct obtain PerfInstrumentation. "
          + "Make sure shared memory for perf data is not turned of by -XX:-UsePerfData.", e);
    }

    //get ticks frequency
    _sunOsHrtFrequency = getSunOsHrtFrequency();
    _tickToNanoMultiplier = TimeUnit.SECONDS.toNanos(1) / _sunOsHrtFrequency;

    _osBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    _threadBean = (ThreadMXBean)ManagementFactory.getThreadMXBean();

    if (!_threadBean.isThreadAllocatedMemorySupported()) {
      throw new RuntimeException("ThreadAllocatedMemory not supported by ThreadMXBean");
    }
    _threadBean.setThreadAllocatedMemoryEnabled(true);
    if (!_threadBean.isThreadAllocatedMemoryEnabled()) {
      throw new RuntimeException("Failed to enable ThreadAllocatedMemory on ThreadMXBean");
    }

    if (!_threadBean.isThreadCpuTimeSupported()) {
      throw new RuntimeException("ThreadCpuTime not supported by ThreadMXBean");
    }
    _threadBean.setThreadCpuTimeEnabled(true);
    if (!_threadBean.isThreadCpuTimeEnabled()) {
      throw new RuntimeException("Failed to enable ThreadCpuTime on ThreadMXBean");
    }

    //install GC notifications listener
    List<MemoryManagerMXBean> memManagerBeans = ManagementFactory.getMemoryManagerMXBeans();
    GCListener listener = new GCListener();
    for (MemoryManagerMXBean bean : memManagerBeans) {
      if (bean.getName().equals("G1 Young Generation") || bean.getName().equals("G1 Old Generation")) {
        NotificationEmitter emitter = (NotificationEmitter) bean;
        emitter.addNotificationListener(listener, null, null);
      }
    }
  }

  private long getSunOsHrtFrequency() {
    for (Counter counter : _perfInstr.getAllCounters()) {
      switch (counter.getName()) {
        case "sun.os.hrt.frequency":
          return valueToLong(counter.getValue());
      }
    }
    throw new RuntimeException("Cound npt find value for performance counter: sun.os.hrt.frequency");
  }

  public long ticksToNano(long ticks) {
    return ticks * _tickToNanoMultiplier;
  }

  public UtilizationSnapshot update() {

    double systemLoadAverage;
    double systemCPULoad;
    long timestamp;
    long ticksTime = -1;
    long totalGcDuration;
    long bytesCollected;
    long[] threadAllocations;
    long[] threadCPUs;
    long[] threadIds;
    long processCPU;
    long youngGCTime = -1;
    long oldGCTime = -1;
    long appTime = -1;
    long safepointTime = -1;
    long vmTime = -1;
    double processCPULoad;

    timestamp = System.nanoTime();
    totalGcDuration = _totalGcDuration.get();
    bytesCollected = _bytesCollected.get();

    for (Counter counter : _perfInstr.getAllCounters()) {
      switch (counter.getName()) {
        case "sun.rt.applicationTime":
          appTime = ticksToNano(valueToLong(counter.getValue()));
          break;
        case "sun.rt.safepointTime":
          safepointTime = ticksToNano(valueToLong(counter.getValue()));
          break;
        case "sun.threads.vmOperationTime":
          vmTime = ticksToNano(valueToLong(counter.getValue()));
          break;
        case "sun.os.hrt.ticks":
          ticksTime = ticksToNano(valueToLong(counter.getValue()));
          break;
        case "sun.gc.collector.0.time":
          youngGCTime = ticksToNano(valueToLong(counter.getValue()));
          break;
        case "sun.gc.collector.1.time":
          oldGCTime = ticksToNano(valueToLong(counter.getValue()));
          break;
      }
    }

    threadIds = _threadBean.getAllThreadIds();
    threadAllocations = _threadBean.getThreadAllocatedBytes(threadIds);
    threadCPUs = _threadBean.getThreadCpuTime(threadIds);

    processCPU = _osBean.getProcessCpuTime();
    processCPULoad = _osBean.getProcessCpuLoad();
    systemCPULoad = _osBean.getSystemCpuLoad();
    systemLoadAverage = _osBean.getSystemLoadAverage();

    return new UtilizationSnapshot(systemLoadAverage, systemCPULoad, timestamp, ticksTime,
        totalGcDuration, bytesCollected, threadAllocations, threadCPUs, threadIds, processCPU, youngGCTime, oldGCTime,
        appTime, safepointTime, vmTime, processCPULoad);
  }

  private Long valueToLong(Object o) {
    return Long.valueOf(String.valueOf(o));
  }

  private class GCListener implements NotificationListener {
    @Override
    public void handleNotification(Notification notification, Object handback) {
      if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
        GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());

        Map<String, MemoryUsage> membefore = info.getGcInfo().getMemoryUsageBeforeGc();
        Map<String, MemoryUsage> mem = info.getGcInfo().getMemoryUsageAfterGc();

        long edenBefore = -1;
        long edenAfter = -1;
        long survivorBefore = -1;
        long survivorAfter = -1;
        long oldBefore = -1;
        long oldAfter = -1;

        for (Map.Entry<String, MemoryUsage> entry: membefore.entrySet()) {
          switch (entry.getKey()) {
            case "G1 Eden Space":
              edenBefore = entry.getValue().getUsed();
              break;
            case "G1 Survivor Space":
              survivorBefore = entry.getValue().getUsed();
              break;
            case "G1 Old Gen":
              oldBefore = entry.getValue().getUsed();
              break;
          }
        }

        for (Map.Entry<String, MemoryUsage> entry: mem.entrySet()) {
          switch (entry.getKey()) {
            case "G1 Eden Space":
              edenAfter = entry.getValue().getUsed();
              break;
            case "G1 Survivor Space":
              survivorAfter = entry.getValue().getUsed();
              break;
            case "G1 Old Gen":
              oldAfter = entry.getValue().getUsed();
              break;
          }
        }

        if (edenBefore >= 0 && edenAfter >= 0 && (edenAfter - edenBefore) >= 0) {
          _bytesCollected.addAndGet(edenAfter - edenBefore);
        }
        if (survivorBefore >= 0 && survivorAfter >= 0 && (survivorAfter - survivorBefore) >= 0) {
          _bytesCollected.addAndGet(survivorAfter - survivorBefore);
        }
        if (oldBefore >= 0 && oldAfter >= 0 && (oldAfter - oldBefore) >= 0) {
          _bytesCollected.addAndGet(oldAfter - oldBefore);
        }

        _totalGcDuration.addAndGet(info.getGcInfo().getDuration() * 1000000L);
      }
    }
  }

  public class UtilizationSnapshot {

    private final double _systemLoadAverage;
    private final double _systemCPULoad;
    private final long _timestamp;
    private final long _ticksTime;
    private final long _totalGcDuration;
    private final long _bytesCollected;
    private final long[] _threadAllocations;
    private final long[] _threadCPUs;
    private final long[] _threadIds;
    private final long _processCPU;
    private final long _youngGCTime;
    private final long _oldGCTime;
    private final long _appTime;
    private final long _safepointTime;
    private final long _vmTime;
    private final double _processCPULoad;

    public UtilizationSnapshot(double systemLoadAverage, double systemCPULoad, long timestamp, long ticksTime,
        long totalGcDuration, long bytesCollected, long[] threadAllocations, long[] threadCPUs, long[] threadIds,
        long processCPU, long youngGCTime, long oldGCTime, long appTime, long safepointTime, long vmTime,
        double processCPULoad) {
      _systemLoadAverage = systemLoadAverage;
      _systemCPULoad = systemCPULoad;
      _timestamp = timestamp;
      _ticksTime = ticksTime;
      _totalGcDuration = totalGcDuration;
      _bytesCollected = bytesCollected;
      _threadAllocations = threadAllocations;
      _threadCPUs = threadCPUs;
      _threadIds = threadIds;
      _processCPU = processCPU;
      _youngGCTime = youngGCTime;
      _oldGCTime = oldGCTime;
      _appTime = appTime;
      _safepointTime = safepointTime;
      _vmTime = vmTime;
      _processCPULoad = processCPULoad;
    }

    public double getSystemLoadAverage() {
      return _systemLoadAverage;
    }

    public double getSystemCPULoad() {
      return _systemCPULoad;
    }

    public long getTimestamp() {
      return _timestamp;
    }

    public long getTicksTime() {
      return _ticksTime;
    }

    public long getTotalGcDuration() {
      return _totalGcDuration;
    }

    public long getBytesCollected() {
      return _bytesCollected;
    }

    public long[] getThreadAllocations() {
      return _threadAllocations;
    }

    public long[] getThreadCPUs() {
      return _threadCPUs;
    }

    public long[] getThreadIds() {
      return _threadIds;
    }

    public long getProcessCPU() {
      return _processCPU;
    }

    public long getYoungGCTime() {
      return _youngGCTime;
    }

    public long getOldGCTime() {
      return _oldGCTime;
    }

    public long getAppTime() {
      return _appTime;
    }

    public long getSafepointTime() {
      return _safepointTime;
    }

    public long getVmTime() {
      return _vmTime;
    }

    public double getProcessCPULoad() {
      return _processCPULoad;
    }
  }
}
