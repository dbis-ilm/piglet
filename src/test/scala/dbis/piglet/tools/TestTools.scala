

package dbis.piglet.tools

import java.net.URI

object TestTools {
  implicit def strToUri(str: String): URI = new URI(str)
}