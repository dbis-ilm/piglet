
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
withURL = FILTER fields BY nonempty(eventcode) and isnum(eventcode) and nonempty(url) ;
domain = FOREACH withURL GENERATE extractDomain(url) as site, (int)eventcode as ecode, avgtone;
myDomains = FILTER domain BY strcontains(LOWER(site), "spiegel.de") or strcontains(LOWER(site),"bbc")  or
                  strcontains(LOWER(site),"welt.de") or strcontains(LOWER(site),"zeit");
grp = GROUP myDomains BY (site, ecode);
avgtones = FOREACH grp GENERATE group as siteecode, avg(myDomains.avgtone) as avgtone
f = FOREACH avgtones GENERATE  siteecode.site as site,siteecode.ecode as code, avgtone

gdelt1 = LOAD '$gdelt' using PigStorage()
fields1 = FOREACH gdelt1 GENERATE $26 as eventcode, (double)$34 as avgtone, $57 as url
withURL1 = FILTER fields1 BY nonempty(eventcode) and isnum(eventcode) and nonempty(url) and (strcontains(url, "spiegel.de") or strcontains(LOWER(url),"bbc")  or strcontains(LOWER(url),"welt.de") or strcontains(LOWER(url),"zeit"));
domain1 = FOREACH withURL1 GENERATE extractDomain(url) as site, (int)eventcode as ecode, avgtone;
grp1 = GROUP domain1 BY (site, ecode)
avgtones1 = FOREACH grp1 GENERATE group as siteecode, avg(domain1.avgtone) as avgtone
f1 = FOREACH avgtones1 GENERATE  siteecode.site as site1,siteecode.ecode as code1, avgtone as avgtone1
j = join f by code, f1 by code1
sameCode = filter j by site != site1 and diff(avgtone, avgtone1) > 5

cnt = Accumulate sameCode GENERATE COUNT(site)
dump cnt
