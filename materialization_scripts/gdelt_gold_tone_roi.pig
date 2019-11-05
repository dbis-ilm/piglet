gdelt = LOAD '$gdelt' using PigStorage()
fields = FOREACH gdelt GENERATE $0 as eventid, $1 as day, $4 as a1code, $5 as a1countrycode, $8 as a1ethniccode, $14 as a2code, $15 as a2countrycode, $18 as a2ethniccode, $29 as goldstein, $33 as avgtone, $39 as a1lat, $40 as a1lon, $47 as a2lat,  $48 as a2lon;
withLoc = FILTER fields BY NONEMPTY(a1lat) and NONEMPTY(a1lon) and NONEMPTY(goldstein) and nonempty(avgtone)
gdeltGeo = FOREACH withLoc GENERATE geometry("POINT("+a1lat+" "+a1lon+")"), (double)goldstein as gold, (double)avgtone as tone;
roi = LOAD '$rgdelt' USING PigStorage(';') as (id: int, wkt: chararray);
roiGeo = FOREACH roi GENERATE geometry(wkt) as geo, id;
toneregion = SPATIAL_JOIN gdeltGeo, roiGeo ON CONTAINEDBY using index rtree(order=5);
toneregionid = FOREACH toneregion GENERATE id, gold, tone;
toneByRegion = GROUP toneregionid BY id
tonePerRegion = FOREACH toneByRegion GENERATE group as regionId, avg(toneregionid.gold), avg(toneregionid.tone)
dump tonePerRegion mute