package dbis.pig.backends.pipefabric

import dbis.pig.backends.CppConfig
import com.typesafe.config.ConfigFactory

object CppConf {
  // loads the cpp configuration file in resources/cpp-compiler.conf
  private val compilerConf = ConfigFactory.load("cpp-compiler")

  def getProperty(s: String) = compilerConf.getString("cpp-compiler." + s)
}

class PipeFabricCompiler extends CppConfig {
  /**
   * Get a C++ compiler, this can be done g++, clang++, ....
   */
  override def getCompiler = CppConf.getProperty("cpp")

  /**
   * Get the libraries which are used during compiling. The compiler has to link to these
   * libraries otherwise, linking error will be shown
   */
  override def getLibraries = (CppConf.getProperty("cpp_libs").split("\\s+") ++ CppConf.getProperty("boost_libs").split("\\s+") ++ CppConf.getProperty("pfabric_libs").split("\\s+")).toList

  /**
   * Get options for compiling the code accordingly such as the optimization level, enabling some
   * macros, etc.
   */
  override def getOptions = CppConf.getProperty("cpp_opts").split("\\s+").toList
  /**
   * Get directories for libraries which are essential during the linking
   */
  override def getLibDirs = (CppConf.getProperty("pfabric_lib_dir").split("\\s+") ++ CppConf.getProperty("boost_lib_dir").split("\\s+")).toList
  /**
   *  Get include directories for finding the header files.
   */
  override def getIncludeDirs = (CppConf.getProperty("cpp_include_dir").split("\\s+") ++ CppConf.getProperty("boost_include_dir").split("\\s+") ++ CppConf.getProperty("pfabric_include_dir").split("\\s+")).toList
}