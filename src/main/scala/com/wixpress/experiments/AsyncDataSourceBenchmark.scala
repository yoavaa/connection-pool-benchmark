package com.wixpress.experiments

import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicInteger
import com.wixpress.hoopoe.asyncjdbc.{AsyncDataSource, QueuedDataSource}
import java.sql._
import concurrent._
import concurrent.duration.Duration
import collection.mutable.ArrayBuffer
import java.lang.System

/**
 *
 * @author yoav
 * @since 5/5/13
 */
class AsyncDataSourceBenchmark {
  private val executor: ExecutorService = Executors.newFixedThreadPool(10)
  private val queueCounter = new BucketCounter
  private val executionCounter = new BucketCounter
  private val errors: AtomicInteger = new AtomicInteger
  private val waterfallReport = new WaterfallReport("AsyncDataSource")

  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private def run {
    val asyncDataSource: QueuedDataSource = QueuedDataSource(Credentials.driver, Credentials.jdbcUrl, Credentials.user, Credentials.pass, 20, 20)

    testConnectionPool(asyncDataSource, true, 0)
    testConnectionPool(asyncDataSource, false,1)
    testConnectionPool(asyncDataSource, false,2)
    testConnectionPool(asyncDataSource, false,3)
    testConnectionPool(asyncDataSource, false,4)
    testConnectionPool(asyncDataSource, false,5)

    executor.shutdown()
    asyncDataSource.shutdown()
  }



  def testConnectionPool(ds: AsyncDataSource, first: Boolean, run: Int) {
    clearTable
    queueCounter.reset
    executionCounter.reset
    errors.set(0)
    waterfallReport.reset
    val start: Long = System.currentTimeMillis
    val index: AtomicInteger = new AtomicInteger(0)
    val futures: ArrayBuffer[Future[Int]] = ArrayBuffer()

    var t: Int = 0
    while (t < 20) {
      var res: Future[Int] = ds.doWithConnection(new AsyncDBOperation(index.incrementAndGet, System.nanoTime))
      var i: Int = 0
      while (i < 1000) {
        res = res.flatMap(new CheckResultAndAsyncDBOperation(index, ds))
        i += 1
      }
      futures += res
      t += 1
    }
    try {
      futures.foreach(Await.ready(_, Duration("100 s")))
    }
    catch {
      case e: InterruptedException => {
        e.printStackTrace()
      }
    }
    if (first)
      println("total time, errors, "+queueCounter.title)
    println("%d,queue,%d,%d,%s".format(run, System.currentTimeMillis - start, errors.get(), queueCounter.formatStatisticsLine))
    println("%d,execution,,,%s".format(run, executionCounter.formatStatisticsLine))
    waterfallReport.printToFile(s"asyncDataSource-$run.html")
  }

  private def clearTable {
    try {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "picobo")
      try {
        val ps: PreparedStatement = conn.prepareStatement("delete from item")
        try {
          ps.execute
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
        println("%d,write, %s".format(index, e.getMessage))
      }
    }
  }

  class AsyncDBOperation(val index: Int,
                         val queueTime: Long) extends (Connection => Int) {
    def apply(v1: Connection): Int = {
      val poolTime = System.nanoTime - queueTime
      queueCounter.report(poolTime)
      val start = System.nanoTime();
      if (index % 10 == 0)
        insert(v1, index)
      else
        read(v1, index)
      val completed = System.nanoTime()
      val executionTime = completed - start
      executionCounter.report(executionTime)
      waterfallReport.addStats(Seq(queueTime, start, completed))
      index
    }
  }

  class CheckResultAndAsyncDBOperation(val index: AtomicInteger,
                                       val asyncDataSource: AsyncDataSource) extends (Int => Future[Int]) {
    def apply(v1: Int): Future[Int] = {
      val idx = index.incrementAndGet
      if (idx < 20000)
        asyncDataSource.doWithConnection(new AsyncDBOperation(idx, System.nanoTime))
      else
        Promise.successful(idx).future
    }

    def isDefinedAt(x: Int) = true
  }
}




object AsyncDataSourceBenchmark extends App {
  Class.forName("com.mysql.jdbc.Driver")
  new AsyncDataSourceBenchmark().run
}
