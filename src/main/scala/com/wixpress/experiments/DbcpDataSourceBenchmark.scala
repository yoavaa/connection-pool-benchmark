package com.wixpress.experiments

import org.apache.commons.dbcp.BasicDataSource

/**
 * 
 * @author yoav
 * @since 5/8/13
 */
class DbcpDataSourceBenchmark extends BaseDataSourceBenchmark[BasicDataSource] {
  def createDataSource: BasicDataSource = {
    val ds = new BasicDataSource()
    ds.setDriverClassName(Credentials.driver)
    ds.setUrl(Credentials.jdbcUrl)
    ds.setUsername(Credentials.user)
    ds.setPassword(Credentials.pass)
    ds.setInitialSize(20)
    ds.setMinIdle(20)
    ds.setMaxIdle(20)
    ds.setMaxActive(20)
    ds
  }

  def closeDataSource(ds: BasicDataSource) {
    ds.close()
  }

  def title = "dbcp"
}

object DbcpDataSourceBenchmark extends App {
  Class.forName("com.mysql.jdbc.Driver")
  new DbcpDataSourceBenchmark().run
}
