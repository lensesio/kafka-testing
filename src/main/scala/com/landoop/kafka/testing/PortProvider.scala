package com.landoop.kafka.testing

import java.net.{InetAddress, ServerSocket}

object PortProvider {
  def appy(count: Int): Vector[Int] = {
    (1 to count).map { _ =>
      val serverSocket = new ServerSocket(0, 0, InetAddress.getLocalHost)
      val port = serverSocket.getLocalPort
      serverSocket.close()
      port
    }.toVector
  }

  def one: Int = appy(1).head
}
