package Concurrency

import java.net.{ServerSocket, Socket}

/**
  * Created by Ding on 9/12/2018.
  */
object ConcurrencyTest {
  def main(args: Array[String]): Unit = {
    new Thread((new NetworkService(2020, 2))).run()
  }
}

class NetworkService(port: Int, poolSize: Int) extends Runnable {
  val serverSocket = new ServerSocket(port)

  override def run(): Unit = while (true) {
    val socket = serverSocket.accept()
    (new Handler(socket)).run()
  }
}

class Handler(socket: Socket) extends Runnable {
  def message = (Thread.currentThread().getName() + "\n").getBytes()

  override def run(): Unit = {
    socket.getOutputStream.write(message)
    socket.getOutputStream.close()
  }
}


