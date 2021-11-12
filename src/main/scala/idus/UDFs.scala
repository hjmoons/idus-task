package idus

object UDFs {
  def re_time(time: String): String = time.replace(" UTC", "")     // timestamp에서 필요없는 문자 제거
  def sec_time(hour: Int, min: Int, sec: Int): Int = hour * 3600 + min * 60 + sec     // timestamp를 초 단위로 변환
  def sess_time(maxTime: Int, minTime: Int): Int = maxTime - minTime                  // 세션 사용 시간

  // 초 단위 시간을 시,분,초 문자열로 변환
  def sec_to_hour(maxTime: Int, minTime: Int): String = {
    val sessTime = maxTime - minTime
    val hour = sessTime / 3600
    val min = (sessTime % 3600) / 60
    val sec = (sessTime % 3600) % 60
    hour.toString + "h " + min.toString + "m " + sec.toString + "s"
  }

  // 15분 단위를 4개로 분류
  def dev_quarter(min: Int): Int = {
    min/15 match {
      case 0 => 0
      case 1 => 1
      case 2 => 2
      case 3 => 3
    }
  }

  // 숫자로 이루어진 시간 단위를 문자열로 변환
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
