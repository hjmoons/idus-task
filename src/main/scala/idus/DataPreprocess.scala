package idus

object DataPreprocess {
  // TimeStamp에서 필요없는 문자 제거
  def re_time(time: String): String = time.replace(" UTC", "")
}
