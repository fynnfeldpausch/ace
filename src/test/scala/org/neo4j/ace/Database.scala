package org.neo4j.acetest

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import scala.sys.ShutdownHookThread

object Database {
  private val path: String = "target/neo4j"
  private val config: Map[String, String] = Map()
  private var dbService: Option[GraphDatabaseService] = None
 
  private def registerShutdownHook(gds: GraphDatabaseService) {
    ShutdownHookThread {
      gds.shutdown()
    }
  }

  def start(): Unit = {
  	require(dbService.isEmpty, "Service already running")
    val service = new GraphDatabaseFactory().newEmbeddedDatabase(path)
    registerShutdownHook(service)
    dbService = Some(service)    
  }

  def shutdown(): Unit = {
  	require(dbService.isDefined, "Service not running")
    dbService.get.shutdown()
    dbService = None
  }

  def service: GraphDatabaseService = {
    require(dbService.isDefined, "Service not running")
    dbService.get
  }

  def withTx[T <: Any](operation: GraphDatabaseService => T): T = {
    val tx = synchronized { service.beginTx }
    try {
      val result = operation(service)
      tx.success()
      result
    } catch {
      case e: Throwable => {
        tx.failure()
        throw new RuntimeException("Database transaction failed", e)
      }
    } finally {
      tx.finish()
    }
  }
}