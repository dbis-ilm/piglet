<%
def getHourPerDay(s: String): String = {
  val t = s.replaceAll("\"","").split('^')(0)
  val dt = t.substring(0, t.lastIndexOf("-"))
  val ldt = java.time.LocalDateTime.parse(dt)
  s"${ldt.getYear}-${ldt.getMonthValue}-${ldt.getDayOfMonth}-${ldt.getHour}"
}
def getFloatValue(s: String): Double = {
  val t = s.split('^')(0)
  t.replaceAll("\"","").toDouble
}
def hash(s: String): Int = { s.hashCode() }
%>

triples = RDFLOAD('$rdffile')
windObs = BGP_FILTER triples BY {
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure>" ?sensor .
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#observedProperty>" "<http://knoesis.wright.edu/ssw/ont/weather.owl#_WindSpeed>" .
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result>" ?resultSubj .
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#samplingTime>" ?timeSubj
}

obsSensor = FOREACH windObs GENERATE sensor, resultSubj , timeSubj;

triples2 = RDFLOAD('$rdffile')
resultsRaw = FILTER triples2 BY predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue>"
resultsWindAll = FOREACH resultsRaw GENERATE subject, getFloatValue(object) as windspeed:double
results = FILTER resultsWindAll BY windspeed >= 74

triples3 = RDFLOAD('$rdffile')
timesRaw = FILTER triples3 BY predicate == "<http://www.w3.org/2006/time#inXSDDateTime>"
times = FOREACH timesRaw GENERATE subject, getHourPerDay(object) as hour:chararray

resultValues = join obsSensor by resultSubj, results by subject
r1 = FOREACH resultValues GENERATE sensor, timeSubj, windspeed;

timeResults = join r1 by timeSubj, times by subject;
fields = FOREACH timeResults GENERATE hash(CONCAT(sensor,hour)) as sensorHour, windspeed;
grp = GROUP fields BY sensorHour;
avgWind = FOREACH grp GENERATE group, AVG(fields.windspeed) as speed;
cnt = accumulate avgWind GENERATE COUNT(speed);
cntStr = FOREACH cnt GENERATE "results" as n1, $0;
dump cntStr