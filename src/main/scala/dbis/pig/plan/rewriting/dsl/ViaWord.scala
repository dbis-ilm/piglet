package dbis.pig.plan.rewriting.dsl

import dbis.pig.plan.rewriting._

import org.kiama.rewriting.Rewriter.strategyf
import scala.reflect.{ClassTag, classTag}

object ViaWord extends Word {
//  def apply[T: ClassTag](f: PigOpToPigOpRewritingFunction[T]): Unit = Rewriter.addInputTypedStrategy(f)

  def apply[T: ClassTag, T2 : ClassTag](f: T => Option[T2]): Unit = {
    val wrapper = { term: Any => term match {
      case _ if classTag[T].runtimeClass.isInstance(term) => f(term.asInstanceOf[T])
      case _ => None
    }
    }
    Rewriter.addStrategy(strategyf(t => wrapper(t)))
  }
}
