<%
def tripDuration(pu: String, dO: String): Long =  {
  import java.time._
  import scala.concurrent.duration._
  val formatter = format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val puDate = LocalDateTime.parse(pu,formatter).toEpochSecond(ZoneOffset.UTC).seconds
  val doDate = LocalDateTime.parse(dO,formatter).toEpochSecond(ZoneOffset.UTC).seconds
  val d = doDate - puDate
  math.abs(d.toSeconds).seconds.toMinutes
}
%>
raw = load '$taxi' using PigStorage(',',skipEmpty=true) as
  (vendor_id:chararray,pickup_datetime:chararray,dropoff_datetime:chararray,passenger_count:chararray,
  trip_distance:chararray, pickup_longitude:chararray,pickup_latitude:chararray,rate_code:chararray,
  store_and_fwd_flag:chararray,dropoff_longitude:chararray,dropoff_latitude:chararray,payment_type:chararray,
  fare_amount:chararray,surcharge:chararray,mta_tax:chararray,tip_amount:chararray,tolls_amount:chararray,total_amount:chararray);

noHeader = filter raw by not STARTSWITH(LOWER(vendor_id),"vendor") and pickup_longitude != "" and pickup_latitude != ""
                                                                   and dropoff_longitude != "" and dropoff_latitude != ""
                                                                   and trip_distance != "";

fields = FOREACH noHeader GENERATE pickup_datetime, dropoff_datetime,
                                    geometry("POINT(" + pickup_latitude +" " + pickup_longitude + ")") as pickuploc,
                                    geometry("POINT(" + dropoff_latitude +" " + dropoff_longitude + ")") as dropoffloc,
                                    (double)trip_distance as trip_distance;

dist_duration = FOREACH fields GENERATE tripDuration(pickup_datetime, dropoff_datetime) as duration:long,
                                        S_DISTANCE(pickuploc, dropoffloc) as distance: double,
                                        trip_distance;

shortdist = FILTER dist_duration BY distance <= 0.02 and duration > 20;

dump shortdist mute