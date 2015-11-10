package dbis.test.pig

import dbis.pig.parser.PigParser.parseScript
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfter
import dbis.pig.plan.MaterializationManager
import dbis.pig.op.Materialize
import java.io.File
import java.io.PrintWriter
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.Path
import scala.collection.JavaConversions._
import java.nio.file.SimpleFileVisitor
import java.net.URI
import com.typesafe.scalalogging.LazyLogging

/**
 * @author hage
 */
class MaterializeTest extends FlatSpec with Matchers with BeforeAndAfter {
  
  
  val baseDir = Files.createTempDirectory("piglet_mat_test")
  val mapFile = Files.createFile(baseDir.resolve("mappings.txt"))
  
  before {
    if(!Files.exists(baseDir))
      Files.createDirectories(baseDir)
      
    val p = new PrintWriter(mapFile.toFile())      
    p.write("")
    p.close()
  }
  
  after {
    recursiveRemove(baseDir)
  }
  
  private def recursiveRemove(dir: Path) {
    
    if (Files.isDirectory(dir)) {
      for(f <- dir.toFile().listFiles()) {
        recursiveRemove(f.toPath())
      }
    }
    
    Files.deleteIfExists(dir)
  }  
  
  
  it should "add store for non existing materialize results" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |materialize b;
         |c = distinct b;
         |""".stripMargin))    
    
    val mm = new MaterializationManager(mapFile, baseDir.toUri())
    
    val plan2 = processMaterializations(plan, mm)
    
    var matOp: Option[Store] = None
    
    for(op <- plan2.operators) {
      op match {
        case f : Store => matOp = Some(f) 
        case _ => 
      }
    }
    
//    plan.sinkNodes.map(PrettyPrinter.pretty).foreach(println)
    
    matOp shouldBe defined
    
  }
  
  it should "add LOAD for existing materialize results" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |materialize b;
         |c = distinct b;
         |""".stripMargin))    
    
    val mat = plan.findOperator { o => o.isInstanceOf[Materialize] }.head match {
      case o: Materialize => o
      case _ => throw new ClassCastException
    }  
      
    // empty the file
    val matResultFile = s"$baseDir${File.separator}blubb.txt"
    val lineage = mat.lineageSignature 
    val p = new PrintWriter(mapFile.toFile())      
    p.write(s"$lineage;$matResultFile\n")
    p.close()
    
    val mm = new MaterializationManager(mapFile, baseDir.toUri())
    
    val plan2 = processMaterializations(plan, mm)
    
//    println(plan2.operators)
    
    var loadOp: Option[Load] = None
    
    /* FIXME: this finds the original load operator, because it was not 
     * removed from the plan
     * 
     * Adding the load just inserts a load and disconnects the subplans
     * However, the now unused subplan still exists and this test finds
     * the unused load operator... :(
     */
    for(op <- plan2.operators) {
      op match {
        case f : Load if f.file != "input/file.csv" => loadOp = Some(f) // bad workaround for the aforementioned issue
        case _ => 
      }
    }
    
//    plan.sinkNodes.map(PrettyPrinter.pretty).foreach(println)
    
    withClue("load operator") {loadOp shouldBe defined}
    
    val op = loadOp.get
    
    withClue("loader file name: ") {op.file.toString() shouldBe matResultFile}
    
    withClue("loader inputs") {op.inputs shouldBe empty}
    
    val c = plan2.findOperatorForAlias("c").get
    withClue("load's successor's input size") {c.inputs.size shouldBe 1}
    withClue("load's successor's input op") {c.inputs(0) shouldBe Pipe("b", op)}
    
  }
}