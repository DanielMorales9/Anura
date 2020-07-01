package com.lsm.server

import com.lsm.engine.AnuraEngine
import com.lsm.thrift.{MyException, Service}
import com.twitter.finagle.Thrift
import com.twitter.util.Await

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object AnuraServer extends App {

  val engine = new AnuraEngine(db_path = "db");

  val service: Service[Future] = new Service[Future] {
    override def put(key: String, value: Int): Future[Unit] = {
      Future {
        engine.put(key, value)
      }
    }

    override def get(key: String): Future[Int] = {
      Future {
        engine.get(key).getOrElse(throw new MyException("Not Found", 404))
      }
    }

    override def delete(key: String): Future[Int] = {
      Future {
        engine.delete(key)
      }
    }

  }
  val server = Thrift.server.serveIface(":1234", service)

  Await.ready(server)
}
