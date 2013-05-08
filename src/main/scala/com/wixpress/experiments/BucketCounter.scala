package com.wixpress.experiments

import java.util.concurrent.atomic.AtomicReference

/**
 * 
 * @author yoav
 * @since 5/8/13
 */
class BucketCounter {
  private val under1000nano = new AtomicReference[Counter](new Counter)
  private val under3200nano = new AtomicReference[Counter](new Counter)
  private val under10micro = new AtomicReference[Counter](new Counter)
  private val under32micro = new AtomicReference[Counter](new Counter)
  private val under100micro = new AtomicReference[Counter](new Counter)
  private val under320micro = new AtomicReference[Counter](new Counter)
  private val under1000micro = new AtomicReference[Counter](new Counter)
  private val under3200micro = new AtomicReference[Counter](new Counter)
  private val under10milli = new AtomicReference[Counter](new Counter)
  private val under32milli = new AtomicReference[Counter](new Counter)
  private val under100milli = new AtomicReference[Counter](new Counter)
  private val under320milli = new AtomicReference[Counter](new Counter)
  private val under1000milli = new AtomicReference[Counter](new Counter)
  private val under3200milli = new AtomicReference[Counter](new Counter)
  private val other = new AtomicReference[Counter](new Counter)

  def report(value: Long) {
    if (value < 1000) inc(under1000nano, value)
    else if (value < 3200) inc(under3200nano, value)
    else if (value < 10000) inc(under10micro, value)
    else if (value < 32000) inc(under32micro, value)
    else if (value < 100000) inc(under100micro, value)
    else if (value < 320000) inc(under320micro, value)
    else if (value < 1000000) inc(under1000micro, value)
    else if (value < 3200000) inc(under3200micro, value)
    else if (value < 10000000) inc(under10milli, value)
    else if (value < 32000000) inc(under32milli, value)
    else if (value < 100000000) inc(under100milli, value)
    else if (value < 320000000) inc(under320milli, value)
    else if (value < 1000000000) inc(under1000milli, value)
    else if (value < 3200000000l) inc(under3200milli, value)
    else inc(other, value)
  }

  def inc(counter: AtomicReference[Counter], value: Long) {
    var oldCounter: Counter = null
    var newCounter: Counter = null
    do {
      oldCounter = counter.get
      newCounter = new Counter(oldCounter.count + 1, oldCounter.sum + value)
    } while (!counter.compareAndSet(oldCounter, newCounter))
  }

  def title: String = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
    "under 1000 nSec",
    "1000 nSec - 3200 nSec",
    "3200 nSec - 10 \u00b5Sec",
    "10 \u00b5Sec - 32 \u00b5Sec",
    "32 \u00b5Sec - 100 \u00b5Sec",
    "100 \u00b5Sec - 320 \u00b5Sec",
    "320 \u00b5Sec - 1000 \u00b5Sec",
    "1000 \u00b5Sec - 3200 \u00b5Sec",
    "3200 \u00b5Sec - 10 mSec",
    "10 mSec - 32 mSec",
    "32 mSec - 100 mSec",
    "100 mSec - 320 mSec",
    "320 mSec - 1000 mSec",
    "1000 mSec - 3200 mSec",
    "other")

  def formatStatisticsLine: String = "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d".format(
    under1000nano.get.count,
    under3200nano.get.count,
    under10micro.get.count,
    under32micro.get.count,
    under100micro.get.count,
    under320micro.get.count,
    under1000micro.get.count,
    under3200micro.get.count,
    under10milli.get.count,
    under32milli.get.count,
    under100milli.get.count,
    under320milli.get.count,
    under1000milli.get.count,
    under3200milli.get.count,
    other.get.count)

  def reset {
    under1000nano.set(new Counter)
    under3200nano.set(new Counter)
    under10micro.set(new Counter)
    under32micro.set(new Counter)
    under100micro.set(new Counter)
    under320micro.set(new Counter)
    under1000micro.set(new Counter)
    under3200micro.set(new Counter)
    under10milli.set(new Counter)
    under32milli.set(new Counter)
    under100milli.set(new Counter)
    under320milli.set(new Counter)
    under1000milli.set(new Counter)
    under3200milli.set(new Counter)
    other.set(new Counter)
  }

  def print {
    println("checkout under 1000 nSec       : " + under1000nano.get.count)
    println("checkout 1000 nSec - 3200 nSec : " + under3200nano.get.count)
    println("checkout 3200 nSec -   10 \u00b5Sec : " + under10micro.get.count)
    println("checkout   10 \u00b5Sec -   32 \u00b5Sec : " + under32micro.get.count)
    println("checkout   32 \u00b5Sec -  100 \u00b5Sec : " + under100micro.get.count)
    println("checkout  100 \u00b5Sec -  320 \u00b5Sec : " + under320micro.get.count)
    println("checkout  320 \u00b5Sec - 1000 \u00b5Sec : " + under1000micro.get.count)
    println("checkout 1000 \u00b5Sec - 3200 \u00b5Sec : " + under3200micro.get.count)
    println("checkout 3200 \u00b5Sec -   10 mSec : " + under10milli.get.count)
    println("checkout   10 mSec -   32 mSec : " + under32milli.get.count)
    println("checkout   32 mSec -  100 mSec : " + under100milli.get.count)
    println("checkout  100 mSec -  320 mSec : " + under320milli.get.count)
    println("checkout  320 mSec - 1000 mSec : " + under1000milli.get.count)
    println("checkout 1000 mSec - 3200 mSec : " + under3200milli.get.count)
    println("checkout other                 : " + other.get.count)
  }

  case class Counter(count: Int = 0, sum: Long = 0L)
}
