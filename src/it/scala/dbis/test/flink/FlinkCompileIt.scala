package dbis.test.flink

import dbis.pig.PigCompiler
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.io.Source

/**
 * Created by philipp on 26.05.15.
 */


class FlinkCompileIt extends FlatSpec with Matchers {
  val scripts = Table(
    ("script", "result", "truth"), // only the header of the table
    ("load.pig", "result1.out", "result1.data"),
    ("load2.pig", "result2.out", "result2.data"),
    ("selfjoin.pig", "joined.out", "joined.data"),
    ("foreach1.pig", "distances.out", "distances.data")
  )

  def cleanupResult(dir: String): Unit = {
    import scalax.file.Path

    val path: Path = Path(dir)
    try {
      path.deleteRecursively(continueOnFailure = false)
    }
    catch {
      case e: java.io.IOException => // some file could not be deleted
    }

  }

  "The Pig compiler" should "compile and execute the script" in {
    forAll(scripts) { (script: String, resultDir: String, truthFile: String) =>
      // 1. make sure the output directory is empty
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig",""))

      // 2. compile and execute Pig script
      PigCompiler.main(Array("--backend", "flink", "--outdir", ".", "./src/it/resources/" + script))
      println("execute: " + script)

      // 3. load the output file and the truth file
      var result = Iterator[String]()
      for (file <- new java.io.File(resultDir).listFiles) {
        result++=Source.fromFile(file).getLines()
      }
      val truth = Source.fromFile("./src/it/resources/" + truthFile).getLines()
      // 4. compare both files
      result.toSeq should contain theSameElementsAs (truth.toTraversable)

      // 5. delete the output directory
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig",""))
    }
  }
}
