package main.scala.com.code.scala

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.LocalDate
object months_diff {
  def main(args: Array[String]): Unit = {

    val input_date_formatter_yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
    val output_date_formatter_yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd")
    val output_date = output_date_formatter_yyyyMMdd.format(input_date_formatter_yyyyMMdd.parse("20220418"))
    println(output_date)


    val diff = ChronoUnit.MONTHS.between(LocalDate.parse(output_date_formatter_yyyyMMdd.format(input_date_formatter_yyyyMMdd.parse("20210418"))),LocalDate.parse(output_date_formatter_yyyyMMdd.format(input_date_formatter_yyyyMMdd.parse("20220418"))))
    println(s"months diff: ${diff}")
  }

}
