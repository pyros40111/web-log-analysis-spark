package utils

import com.typesafe.config.{Config, ConfigFactory}

class EnvConfig(config: Config) {
  // data configs
  val LOG_FILE_PATH = config.getString("data.path")
  // spark configs
  val SPARK_APP_NAME = config.getString("spark.appName")
  val SPARK_MASTER = config.getString("spark.master")
  // web log analysis config
  val SESSION_EXPIRE_MILLISECONDS = config.getInt("weblog.sessionExpireMilliseconds")
}


object EnvConfig {
  private var _envConfig: EnvConfig = _

  def getEnvConfig(): EnvConfig = {
    _envConfig
  }

  def loadLocalConfig(path: String): Unit = {
    _envConfig = new EnvConfig(ConfigFactory.load(path))
  }
}
