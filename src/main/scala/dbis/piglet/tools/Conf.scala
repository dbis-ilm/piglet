package dbis.piglet.tools

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.typesafe.config.{Config, ConfigFactory}
import dbis.piglet.mm.{CostStrategy, EvictionStrategy, GlobalStrategy, ProbStrategy}
import dbis.piglet.op.CacheMode
import dbis.piglet.tools.logging.PigletLogging

import scala.concurrent.duration._

/**
 * This is the global configuration object that contains all user-defined values
 */
object Conf extends PigletLogging {

  val EXECTIMES_FRAGMENT = "times"
  val SIZES_FRAGMENT = "sizes"
  val MATERIALIZATION_FRAGMENT = "materializations"


	val programHome = Paths.get(System.getProperty("user.home"), ".piglet")

  val cacheSizePattern = "([0-9]+)\\s*(?i)(k|m|g|kb|mb|gb)".r


	// create directory if not exists
	if(!Files.exists(programHome))
		Files.createDirectory(programHome)

  /**
   * The path to the config file. It will resolve to $USER_HOME/.piglet/piglet.conf
   */
  private val configFile = "piglet.conf"

  /**
   * Load the configuration.
   *
   * This loads the configuration from the user's home directory. If the config file cannot be found
   * (see [[Conf#configFile]]) the default values found in src/main/resources/piglet.conf are
   * copied to [[Conf#configFile]]
   *
   * @return Returns the config object
   */
  private def loadConf: Config = {

	  val userConf = programHome.resolve(configFile)

    if(Files.exists(userConf)) {

      // if the config file exists in the program home, use this one
      logger.info(s"using $userConf as config file")
      ConfigFactory.parseFile(userConf.toFile)

    } else {
      // Otherwise, use the packaged one
      logger.info(s"loading default packaged config file")
      ConfigFactory.load(configFile)
    }
  }

  protected[piglet] def copyConfigFile() = {
    val source = Conf.getClass.getClassLoader.getResourceAsStream(configFile)
    val dest = programHome.resolve(configFile)

    if(Files.exists(dest)) {
      val bak = new File(s"${dest.toAbsolutePath.toString}.bak").toPath
      logger.debug(s"create bakup file as $bak")

      Files.copy(dest, bak, StandardCopyOption.REPLACE_EXISTING)
    }

    Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING)
    logger.debug(s"copied config file to $dest")
  }

  // loads the configuration file
  private lazy val appconf = loadConf

  def replHistoryFile = programHome.resolve(appconf.getString("repl.history"))

  def materializationBaseDir = new URI(appconf.getString("materialization.basedir"))
  lazy val materializationMapFile = programHome.resolve(Paths.get(appconf.getString("materialization.mapfile"))).toAbsolutePath


  def defaultBackend = appconf.getString("backends.default")

  def backendJar(backend: String): Path = Paths.get(appconf.getString(s"backends.$backend.jar"))

  def backendConf(backend: String) = appconf.getString(s"backends.$backend.conf")

  // TODO: shouldn't we move this to the backend config? (not in Piglet itself)
  def backendGenerator(backend: String) = appconf.getString(s"backends.$backend.generator.class")
  def backendExtension(backend: String) = appconf.getString(s"backends.$backend.generator.extension")
  def backendCompileConf(backend: String) = appconf.getString(s"backends.$backend.compileconf")

  def hdfsCoreSiteFile = Paths.get(appconf.getString("hdfs.coresite"))
  def hdfsHdfsSiteFile = Paths.get(appconf.getString("hdfs.hdfssite"))

  def commonJar = Paths.get(appconf.getString("common.jar"))

  def spatialJar = Paths.get(appconf.getString("features.spatial.jar"))

  def statServerPort = appconf.getInt("statserver.port")
  def statServerURL = if(appconf.hasPath("statserver.url")) Some(URI.create(appconf.getString("statserver.url"))) else None


  def profilingFile = appconf.getString("profiler.configfile")
  def mmDefaultCostStrategy = CostStrategy.withName(appconf.getString("profiler.defaults.cost_strategy").toUpperCase)
  def mmDefaultProbStrategy = ProbStrategy.withName(appconf.getString("profiler.defaults.prob_strategy").toUpperCase)
  def mmDefaultStrategy = GlobalStrategy.withName(appconf.getString("profiler.defaults.global_strategy").toUpperCase)

  def mmDefaultEvictionStrategy = EvictionStrategy.withName(appconf.getString("profiler.defaults.cache.eviction").toUpperCase)
  def mmDefaultCacheSize = {

    val s = appconf.getString("profiler.defaults.cache.size")

    val cacheSizeBytes = s match {
      case cacheSizePattern(num, unit) =>
        val n = num.toLong

        val power = unit.toLowerCase match {
          case null =>
            0
          case "k" | "kb" =>
            1
          case "m" | "mb" =>
            2
          case "g" | "gb" =>
            3
        }

        (n * math.pow(1024,power)).toLong
      case _ =>
        Long.MaxValue
    }

    cacheSizeBytes

  }

  def mmDefaulAdmissionCheck: Boolean = appconf.getBoolean("profiler.defaults.cache.admissioncheck")

  def mmDefaultProbThreshold = appconf.getDouble("profiler.defaults.prob_threshold")


  def mmDefaultMinBenefit: Duration = {
    if(appconf.getString("profiler.defaults.benefit").equalsIgnoreCase("undefined"))
      Duration.Undefined
    else {
      val javaDuration = appconf.getDuration("profiler.defaults.benefit")
      Duration.fromNanos(javaDuration.toNanos)
    }
  }

  def mmDefaultCacheMode = CacheMode.withName(appconf.getString("profiler.defaults.cache_mode").toUpperCase)
  def mmDefaultFraction = appconf.getInt("profiler.defaults.fraction")

  val MiBPerSecWriting = appconf.getDouble("profiler.mibpersecwriting") //93.18239726276708 // * 1024 * 1024 // MB/sec --> Bytes/sec
  val MiBPerSecReading = appconf.getDouble("profiler.mibpersecreading") //114.56189027118947 // * 1024 * 1024 // MB/sec --> Bytes/sec

  val BytesPerSecWriting = MiBPerSecWriting * 1024 * 1024 // MB/sec --> Bytes/sec
  val BytesPerSecReading = MiBPerSecReading * 1024 * 1024 // MB/sec --> Bytes/sec

//  def execTimesFile = "exectimes"
  
//  def langfeatureImports(feature: String) = appconf.getStringList(s"langfeature.$feature.imports").asScala
//  def langfeatureAdditionalJars(feature: String) = appconf.getStringList(s"langfeature.$feature.jars")



}
