<%
def getFloatValue(s: String): Double = {
  val t = s.split('^')(0)
  t.replaceAll("\"","").toDouble
}
%>

snsrs = LOAD '$sensorsflat' using PigStorage(';') AS (sname, lat: double, lng: double);

sLoc = FOREACH snsrs GENERATE geometry("POINT("+lng+" "+lat+")") as loc, sname;

-- regions of interest
roiRaw = LOAD '$roi' using PigStorage(';') as (rname, wkt: chararray);
roi = FOREACH roiRaw GENERATE geometry(wkt) as region, rname;

-- spatial join: sensors in ROI
snsrsOfIntrst = SPATIAL_JOIN sLoc, roi ON CONTAINEDBY using index RTree(order=5);

senDetails = FOREACH snsrsOfIntrst GENERATE sname, rname;

observations = RDFLOAD('$rdffile')

tempObs = BGP_FILTER observations BY {
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure>" ?sensor .
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#observedProperty>" "<http://knoesis.wright.edu/ssw/ont/weather.owl#_AirTemperature>" .
  ?obs "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result>" ?resultSubj
}

obsSensor = FOREACH tempObs GENERATE sensor, resultSubj;

triples = RDFLOAD('$rdffile')
resultsRaw = FILTER triples BY predicate == "<http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue>"
results = FOREACH resultsRaw GENERATE subject, getFloatValue(object) as temperature:double

resultValues = join obsSensor by resultSubj, results by subject
temps = FOREACH resultValues GENERATE sensor,  temperature;

obsROI = JOIN temps BY sensor, senDetails BY sname;
obsDetail = FOREACH obsROI GENERATE rname, temperature;

regGrp = GROUP obsDetail BY rname;
stats = FOREACH regGrp
        GENERATE group AS region,
                 MIN(obsDetail.temperature) AS minTemp,
                 MAX(obsDetail.temperature) AS maxTemp,
                 AVG(obsDetail.temperature) AS avgTemp;

DUMP stats;
