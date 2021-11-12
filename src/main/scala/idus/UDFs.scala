package idus

object UDFs {
  // TimeStamp에서 필요없는 문자 제거
  def re_time(time: String): String = time.replace(" UTC", "")
  def sec_time(hour: Int, min: Int, sec: Int): Int = hour * 3600 + min * 60 + sec
  def sess_time(maxTime: Int, minTime: Int): Int = maxTime - minTime
  def sec_to_hour(maxTime: Int, minTime: Int): String = {
    val sessTime = maxTime - minTime
    val hour = sessTime / 3600
    val min = (sessTime % 3600) / 60
    val sec = (sessTime % 3600) % 60
    hour.toString + "h " + min.toString + "m " + sec.toString + "s"
  }
  def dev_quarter(min: Int): Int = {
    min/15 match {
      case 0 => 0
      case 1 => 1
      case 2 => 2
      case 3 => 3
    }
  }
  def set_quarter_time(date: String, hour: Int, quarter: Int): String = {
    if (hour < 10) {
      if (quarter == 0)
        date + " 0" + hour.toString + ":0" + (quarter * 15).toString + ":00"
      else
        date + " 0" + hour.toString + ":" + (quarter * 15).toString + ":00"
    } else
      date + " " + hour.toString + ":" + (quarter * 15).toString + ":00"
  }
}
