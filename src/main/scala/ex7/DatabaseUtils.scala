package ex7

import java.sql.Connection
import java.util.Properties

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.adapters.H2Adapter
import org.squeryl.{Session, SessionFactory}

object DatabaseUtils {

  def openSession() = {
    // load configuration properties
    val configuration: Properties = new Properties()
    configuration.load(getClass.getClassLoader.getResourceAsStream("conf.properties"))

    Class.forName(configuration.getProperty("database.driver"))
    SessionFactory.concreteFactory = Some { () =>
      Session.create(
        java.sql.DriverManager.getConnection(
          configuration.getProperty("database.url"),
          configuration.getProperty("database.user"),
          configuration.getProperty("database.password")),
        new H2Adapter
      )
    }
  }

  def createConnection(): Option[Connection] = {
    // load configuration properties
    val configuration: Properties = new Properties()
    configuration.load(getClass.getClassLoader.getResourceAsStream("conf.properties"))

    Class.forName(configuration.getProperty("database.driver"))
    Some(java.sql.DriverManager.getConnection(
      configuration.getProperty("database.url"),
      configuration.getProperty("database.user"),
      configuration.getProperty("database.password")))
  }

  def createSchemaIfNotExists() = {
    // check if table exists using direct jdbc
    // unfortunately there is no possibility to do this with squeryl

    var tableExists = false

    var connection: Option[Connection] = None
    try {
      connection = createConnection()
      val resultSet = connection.get.getMetaData.getTables(null, null, DatabaseSchema.STATISTICS_TABLE, null)
      if (resultSet.next()) {
        tableExists = true
      }
    } finally {
      if (connection.isDefined) {
        connection.get.close()
      }
    }

    if (!tableExists) {
      transaction {
        println("Creating schema...")
        DatabaseSchema.create
      }
    }
  }

}
