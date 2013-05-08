package com.wixpress.experiments

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutionException, Future, Executors, ExecutorService}
import javax.sql.DataSource
import collection.mutable.ArrayBuffer
import java.sql._

/**
 *
 * @author yoav
 * @since 5/7/13
 */
abstract class BaseDataSourceBenchmark[DataSourceType <: DataSource] {
  private val acquireCounter = new BucketCounter
  private val releaseCounter = new BucketCounter
  private val overheadCounter = new BucketCounter
  private val executionCounter = new BucketCounter
  private val errors: AtomicInteger = new AtomicInteger
  private val executor: ExecutorService = Executors.newFixedThreadPool(20)
  private val waterfallReport = new WaterfallReport(title)

  def run {
    val ds: DataSourceType = createDataSource

    testConnectionPool(ds, true,0)
    testConnectionPool(ds, false,1)
    testConnectionPool(ds, false,2)
    testConnectionPool(ds, false,3)
    testConnectionPool(ds, false,4)
    testConnectionPool(ds, false,5)

    executor.shutdown()
    closeDataSource(ds)
  }

  def title: String
  def createDataSource: DataSourceType
  def closeDataSource(ds: DataSourceType)

  def testConnectionPool(ds: DataSource, first: Boolean, run: Int) {
    clearTable(ds)
    acquireCounter.reset
    releaseCounter.reset
    executionCounter.reset
    overheadCounter.reset
    errors.set(0)
    waterfallReport.reset
    val start: Long = System.currentTimeMillis
    val futures: ArrayBuffer[Future[_]] = new ArrayBuffer[Future[_]]
    var i = 0
    while (i < 20000) {
      futures += executor.submit(new MyRunnable(i, ds))
      i += 1
    }
    for (f <- futures) try {
      f.get
    }
    catch {
      case e: InterruptedException => {
      }
      case e: ExecutionException => {
      }
    }
    if (first)
      println("run, param, total time, errors, "+acquireCounter.title)
    println("%d, aquire,%d,%d,%s".format(run, System.currentTimeMillis - start, errors.get(), acquireCounter.formatStatisticsLine))
    println("%d, execution, , ,%s".format(run, executionCounter.formatStatisticsLine))
    println("%d, release, , ,%s".format(run, releaseCounter.formatStatisticsLine))
    println("%d, overhead, , ,%s".format(run, overheadCounter.formatStatisticsLine))
    waterfallReport.printToFile(s"$title-$run.html")
  }

  class MyRunnable(index: Int, ds: DataSource) extends Runnable {
    def run() {
      val start = System.nanoTime
      val conn = ds.getConnection
      val startExecution = System.nanoTime()

      val aquireTime: Long = startExecution - start
      acquireCounter.report(aquireTime)
      try {
        if (index % 10 == 0)
          insert(conn, index)
        else
          read(conn, index)
      }
      finally {
        val startRelease = System.nanoTime()
        executionCounter.report(startRelease - startExecution)
        conn.close()
        val completedRelease = System.nanoTime()

        val releaseTime: Long = completedRelease - startRelease
        releaseCounter.report(releaseTime)
        overheadCounter.report(aquireTime + releaseTime)
        waterfallReport.addStats(Seq(start, startExecution, startRelease, completedRelease))
      }
    }

  }

  private def clearTable(ds: DataSource) {
    try {
      //      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "picobo")
      val conn: Connection = ds.getConnection
      try {
        val ps: PreparedStatement = conn.prepareStatement("delete from item")
        try {
          ps.execute
          println("table cleared")
        }
        finally {
          ps.close()
        }
      }
      finally {
        conn.close()
      }
    }
    catch {
      case e: SQLException => {
        e.printStackTrace()
      }
    }
  }

  private def read(conn: Connection, index: Int) {
    try {
      val ps: PreparedStatement = conn.prepareStatement("select * from item")
      try {
        val rs: ResultSet = ps.executeQuery
        try {
          var count: Int = 0
          while (rs.next) {
            count += 1
          }
        }
        finally {
          rs.close()
        }
      }
      finally {
        ps.close()
      }
    }
    catch {
      case e: SQLException => {
        errors.incrementAndGet
        println("%d,read,  %s".format(index, e.getMessage))
      }
    }
  }

  private def insert(conn: Connection, index: Int) {
    try {
      val ps: PreparedStatement = conn.prepareStatement("insert into item (file_name, user_guid, media_type, date_created, date_updated) values (?, ?, ?, ?, ?)")
      try {
        ps.setString(1, "file" + index)
        ps.setString(2, "guid" + index)
        ps.setString(3, "pic" + index)
        ps.setDate(4, new Date(System.currentTimeMillis))
        ps.setDate(5, new Date(System.currentTimeMillis))
        ps.execute
      }
      finally {
        ps.close()
      }
    }
    catch {
      case e: SQLException => {
        errors.incrementAndGet
        println("%d,write, %s, %s, %s".format(index, e.getMessage, e.getClass, e.getStackTrace()(0)))
      }
    }
  }

}
