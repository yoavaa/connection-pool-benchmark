package com.wixpress.experiments

import com.jolbox.bonecp.BoneCPDataSource

/**
 * 
 * @author yoav
 * @since 5/6/13
 */
class BoneDataSourceBenchmark extends BaseDataSourceBenchmark[BoneCPDataSource] {

  def createDataSource: BoneCPDataSource = {
    val ds: BoneCPDataSource = new BoneCPDataSource
    ds.setJdbcUrl(Credentials.jdbcUrl)
    ds.setUsername(Credentials.user)
    ds.setPassword(Credentials.pass)
    ds.setPartitionCount(2)
    ds.setMinConnectionsPerPartition(10)
    ds.setMaxConnectionsPerPartition(10)
    ds
  }

  def closeDataSource(ds: BoneCPDataSource) {
    ds.close()
  }

  def title = "bone"
}

object BoneDataSourceBenchmark extends App {
  Class.forName("com.mysql.jdbc.Driver")
  new BoneDataSourceBenchmark().run
}
