import java.time.Instant

import com.tutorial.weblog.{Main, WebLog}
import org.scalatest._

class WebLogSpec extends FlatSpec with Matchers {
  it should "parse weblog without errors" in {
    val parsedLog = Main.parseToWebLog("2015-07-22T09:00:28.019497Z marketpalce-shop 107.167.109.204:38565 10.0.4.150:80 0.000023 0.072252 0.000021 302 302 189 74 \"POST https://paytm.com:443/ HTTP/1.1\" \"Opera/9.80 (Windows Phone; Opera Mini/8.0.4/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1")
    val result = parsedLog match {
      case wl: WebLog => true
      case _ => false
    }
    assert(result)
  }

  it should "parse weblog correctly" in {
    val parsedLog = Main.parseToWebLog("2015-07-22T09:00:28.019497Z marketpalce-shop 107.167.109.204:38565 10.0.4.150:80 0.000023 0.072252 0.000021 302 302 189 74 \"POST https://paytm.com:443/ HTTP/1.1\" \"Opera/9.80 (Windows Phone; Opera Mini/8.0.4/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1")
    assert(parsedLog.ip == "107.167.109.204")
    assert(parsedLog.timestamp == "2015-07-22T09:00:28.019497Z")
    assert(parsedLog.time == Instant.parse("2015-07-22T09:00:28.019497Z"))
  }
}