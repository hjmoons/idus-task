package idus

object UDFs {
  // TimeStamp에서 필요없는 문자 제거
  def re_time(time: String): String = time.replace(" UTC", "")
  def sec_time(hour: Int, min: Int, sec: Int): Int = hour * 3600 + min * 60 + sec
  def sess_time(maxTime: Int, minTime: Int): Int = maxTime - minTime
}
