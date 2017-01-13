package dbis.piglet.tools

object HdfsCommand extends Enumeration {
  type HdfsCommand = Value
  val COPYTOLOCAL,
      COPYTOREMOTE,
      RM,
      RMDIR,
      MKDIR,
      LS,
      CAT,
      GETMERGE = Value
      
}