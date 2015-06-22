package db

import java.sql.Blob
import slick.driver.H2Driver

// http://slick.typesafe.com/doc/3.0.0/schemas.html


/**
 * Table definitions.
 */
object Tables {

  import H2Driver.api._

  /**
   * Definition of the UPLOAD table.
   */
  class Upload(tag: Tag) extends Table[(Int, String, Blob)](tag, "UPLOAD") {
    def id = column[Int]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")
    def data = column[Blob]("DATA")
    def * = (id, name, data)
  }
  val uploads = TableQuery[Upload]

}
