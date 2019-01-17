import java.sql.{Connection, DriverManager}


object OracleDBUtils {

  val url = "jdbc:oracle:thin:@172.16.230.11:1521:zdxdb"
  val username = "frontbank"
  val password = "sdff23s"

  classOf[oracle.jdbc.driver.OracleDriver]

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try{
      if(!conn.isClosed() || conn != null){
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

}
