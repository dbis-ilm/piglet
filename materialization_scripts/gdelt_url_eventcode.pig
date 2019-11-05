<%
def extractDomain(url: String): String = {
  if(!url.startsWith("http"))
    url
  else {
    val startPos = url.indexOf("//")+2
    val endPos = if(url.indexOf("/",startPos) < 0) { url.size } else { url.indexOf("/",startPos) }
    url.substring(startPos, endPos)
  }
}
def diff(d1: Double, d2: Double): Double = {
  math.abs(d1 - d2)
}
def isnum(s: String): Boolean = {
  scala.util.Try {
      s.toDouble
    }.map(_ => true).getOrElse(false)
}
%>
gdelt = LOAD '$gdelt' using PigStorage();
fields = FOREACH gdelt GENERATE $26 as eventcode, (double)$34 as avgtone, $57 as url;
withURL = FILTER fields BY nonempty(eventcode) and isnum(eventcode) and nonempty(url)
domain = FOREACH withURL GENERATE extractDomain(url) as site, (int)eventcode as ecode, avgtone;
grp = GROUP domain BY (site, ecode);
avgtones1 = FOREACH grp GENERATE group as siteecode, avg(domain.avgtone) as avgtone
avgtones = FILTER avgtones1 BY avgtone != 0
f = FOREACH avgtones GENERATE  siteecode.site as site,siteecode.ecode as code, avgtone
ordered = ORDER f BY site, code
dump ordered mute;