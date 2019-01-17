import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 操作数据库的相关方法
 */
class Operation_record {

  case class Record(id: Int, max_id: Int)
  case class IsExeRecord(id: Int, is_exe: Int)
  case class SignalRecord(id: Int, signal: Int)

  //Update operator这里是更新最大的id
  def modify(record: Record,operation_record_table:String): Boolean ={
    val conn = DBUtils.getConnection()
    try{
      val sql = " UPDATE "+ operation_record_table +" SET max_id = ? ,record_time=now() WHERE id = ?"
      val pstm = conn.prepareStatement(sql)
      pstm.setObject(1, record.max_id)
      pstm.setObject(2, record.id)
      pstm.executeUpdate() > 0
    }
    finally {
      DBUtils.close(conn)
    }
  }
  //这里是更新在执行完第一次将是否执行过赋值为1，做更新


  def modifyisexe(isexerecord: IsExeRecord,operation_record_table:String): Boolean ={
    val conn = DBUtils.getConnection()
    try{
      val sql = " UPDATE "+ operation_record_table +" SET is_exe = ? ,record_time=now() WHERE id = ?"
      val pstm = conn.prepareStatement(sql)
      pstm.setObject(1, isexerecord.is_exe)
      pstm.setObject(2, isexerecord.id)
      pstm.executeUpdate() > 0
    }
    finally {
      DBUtils.close(conn)
    }
  }


  def modifysignalrecord(signalrecord: SignalRecord,operation_record_table:String): Boolean ={
    val conn = DBUtils.getConnection()
    try{
      val sql = " UPDATE "+ operation_record_table +" SET `signal` = ? ,record_time=now() WHERE id = ?"
      val pstm = conn.prepareStatement(sql)
      pstm.setObject(1, signalrecord.signal)
      pstm.setObject(2, signalrecord.id)
      pstm.executeUpdate() > 0
    }
    finally {
      DBUtils.close(conn)
    }
  }


  //获取最大的id
  def getmaxId(tableName:String): Long ={

    var result:Long=0
    val conn = OracleDBUtils.getConnection()
    try{
      val sql = "SELECT MAX(ID) as maxid  FROM "+ tableName
      val pstm = conn.prepareStatement(sql)
      val rs= pstm.executeQuery()

      val rsmd = rs.getMetaData()
      val size = rsmd.getColumnCount()
      val buffer = new ArrayBuffer[mutable.HashMap[String, Any]]()
      while (rs.next()){
        val map = mutable.HashMap[String, Any]()
        for(i <- 1 to size) {
          map += (rsmd.getColumnLabel(i) -> rs.getString(i))
        }
        buffer += map
      }
      val array=buffer.toArray
      val option = array(0).get(rsmd.getColumnLabel(1))
      val maxid= option match {case Some(x)=> x}
      result=maxid.toString.toLong
    }
    finally {
      OracleDBUtils.close(conn)
    }
    result
  }

  //获取最大的id
  def getminId(tableName:String): Long ={

    var result:Long=0
    val conn = OracleDBUtils.getConnection()
    try{
      val sql = "SELECT MIN(ID) as minid  FROM "+ tableName
      val pstm = conn.prepareStatement(sql)
      val rs= pstm.executeQuery()

      val rsmd = rs.getMetaData()
      val size = rsmd.getColumnCount()
      val buffer = new ArrayBuffer[mutable.HashMap[String, Any]]()
      while (rs.next()){
        val map = mutable.HashMap[String, Any]()
        for(i <- 1 to size) {
          map += (rsmd.getColumnLabel(i) -> rs.getString(i))
        }
        buffer += map
      }
      val array=buffer.toArray
      val option = array(0).get(rsmd.getColumnLabel(1))
      val maxid= option match {case Some(x)=> x}
      result=maxid.toString.toLong
    }
    finally {
      OracleDBUtils.close(conn)
    }
    result
  }


  //获取操作表当中最大的id
  def getctmaxId(tableName:String): Long ={

    var result:Long=0
    val conn = OracleDBUtils.getConnection()
    try{
      val sql = "SELECT MAX(\"id\") as maxid  FROM "+ tableName
      val pstm = conn.prepareStatement(sql)
      val rs= pstm.executeQuery()

      val rsmd = rs.getMetaData()
      val size = rsmd.getColumnCount()
      val buffer = new ArrayBuffer[mutable.HashMap[String, Any]]()
      while (rs.next()){
        val map = mutable.HashMap[String, Any]()
        for(i <- 1 to size) {
          map += (rsmd.getColumnLabel(i) -> rs.getString(i))
        }
        buffer += map
      }
      val array=buffer.toArray
      val option = array(0).get(rsmd.getColumnLabel(1))
      val maxid= option match {case Some(x)=> x}
      result=maxid.toString.toLong
    }
    finally {
      OracleDBUtils.close(conn)
    }
    result
  }

}
