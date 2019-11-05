<%
def getFloatValue(s: String): Double = {
  val t = s.split('^')(0)
  t.replaceAll("\"","").toDouble
}
def getDateOfObs(s: String): Int = {
  s.split("_").slice(3,8).mkString("_").hashCode()
}
%>

triples = RDFLOAD('$rdffile')

obsWind = FILTER triples BY predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation>" and STRCONTAINS(object, "_WindSpeed")
obs1 = FOREACH obsWind GENERATE subject as sensor1, object as observation1

triples0 = RDFLOAD('$rdffile')
obsTemp = FILTER triples0 BY predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation>" and STRCONTAINS(object, "_AirTemperature")
obs2 = FOREACH obsTemp GENERATE subject as sensor2, object as observation2

obsSensor = JOIN obs1 BY sensor1, obs2 BY sensor2
obsTempWind = FILTER obsSensor BY observation1 != observation2 and getDateOfObs(observation1) == getDateOfObs(observation2)

observations1 = FOREACH obsTempWind GENERATE sensor1, observation1, observation2
observations = DISTINCT observations1

triples1 = RDFLOAD('$rdffile')
snowFallObs1 = FILTER triples1 BY predicate == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" and
                                object == "<http://knoesis.wright.edu/ssw/ont/weather.owl#SnowfallObservation>"
snowFallObs = FOREACH snowFallObs1 GENERATE subject as obs;

triples2 = RDFLOAD('$rdffile')
snowFallGenObs1 = FILTER triples2 BY predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation>"
snowFallGenObs = FOREACH snowFallGenObs1 GENERATE subject as sensor, object as obs1;

snowFallBGP = JOIN snowFallObs by obs, snowFallGenObs by obs1;
snowsensors1 = FOREACH snowFallBGP GENERATE sensor as snowSensor ;
snowsensors = DISTINCT snowsensors1

snowObservations = JOIN observations BY sensor1, snowsensors by snowSensor;
snowObs = FOREACH snowObservations GENERATE sensor1, observation1, observation2;

triples3 = RDFLOAD('$rdffile')
tempBGP = FILTER triples3 BY STRCONTAINS(LOWER(subject),"airtemperature") and predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result>"
tempObservation = FOREACH tempBGP GENERATE subject as tempObs, object as tempResult;

triples4 = RDFLOAD('$rdffile')
windBGP = FILTER triples4 BY STRCONTAINS(LOWER(subject),"windspeed")  and predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result>"
windObservation = FOREACH windBGP GENERATE subject as windObs, object as windResult;

triples5 = RDFLOAD('$rdffile')
resultsRaw = FILTER triples5 BY predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue>"
resultsAll = FOREACH resultsRaw GENERATE subject as result, getFloatValue(object) as resvalue:double


tempValueAll = JOIN tempObservation BY tempResult, resultsAll by result;
tempvalue = FOREACH tempValueAll GENERATE tempObs, resvalue as temperature;

windValueAll = JOIN windObservation BY windResult, resultsAll by result
windvalue = FOREACH windValueAll GENERATE windObs, resvalue as windspeed;
snowTempAll = JOIN snowObs BY observation2, tempvalue by tempObs;
snowTemp = FOREACH snowTempAll GENERATE sensor1, temperature, observation1
snowTempWindAll = JOIN snowTemp BY observation1, windvalue by windObs;
snowTempWind = FOREACH snowTempWindAll GENERATE sensor1, temperature, windspeed
sensorStatsGrp = GROUP snowTempWind BY sensor1
sensorStats = FOREACH sensorStatsGrp GENERATE group, AVG(snowTempWind.windspeed), MIN(snowTempWind.temperature)
dump sensorStats