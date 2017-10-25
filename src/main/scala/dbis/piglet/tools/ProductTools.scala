package dbis.piglet.tools

case class ProductTools(p: Product) {
  def mkString(sep: String = ",") = p.productIterator.mkString(sep)
}

object ProductTools {
  implicit def productMkString(p: Product): ProductTools = ProductTools(p)
}
