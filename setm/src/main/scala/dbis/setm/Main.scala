package dbis.setm

import dbis.setm.SETM._

object Main {

    def myFunction(s: String) = timing("greeting func") {
      // complex operations, e.g.
      (0 until 100).foreach(i => println(s"Hello $s"))
    }

    def main(args: Array[String]) {

      timing("program total") {

        val names = timing("create names") { Array("Tick","Trick","Track") }

        for(name <- names)
          myFunction(name)

      }

      collect()
    }
}
