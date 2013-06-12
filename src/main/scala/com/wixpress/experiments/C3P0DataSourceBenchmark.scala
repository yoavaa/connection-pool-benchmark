package com.wixpress.experiments

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
 *
 * @author yoav
 * @since 5/6/13
 */
class C3P0DataSourceBenchmark extends BaseDataSourceBenchmark[ComboPooledDataSource]{

  def createDataSource: ComboPooledDataSource = {
    val ds = new ComboPooledDataSource()
    ds.setDriverClass(Credentials.driver)
    ds.setJdbcUrl(Credentials.jdbcUrl)
    ds.setUser(Credentials.user)
    ds.setPassword(Credentials.pass)
    ds.setMinPoolSize(20)
    ds.setInitialPoolSize(20)
    ds.setMaxPoolSize(20)
    ds.setAcquireIncrement(10)
    ds.setNumHelperThreads(6)
    ds
  }

  def closeDataSource(ds: ComboPooledDataSource) {
    ds.close()
  }

  def title = "c3p0"
}

object C3P0DataSourceBenchmark extends App {
  Class.forName("com.mysql.jdbc.Driver")
  new C3P0DataSourceBenchmark().run
}

