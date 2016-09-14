package ex7

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.{Schema, Table}

object DatabaseSchema extends Schema {

  val STATISTICS_TABLE = "STATISTICS"

  val STATISTICS: Table[Statistic] = table[Statistic](STATISTICS_TABLE)

  on(STATISTICS)(s => declare(
    s.month is indexed,
    s.browser is indexed,
    s.os is indexed,
    columns(s.month, s.browser, s.os) are unique
  ))

}
