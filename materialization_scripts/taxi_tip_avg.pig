<%
def dateToMonth(date: String): Int = {
  val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  java.time.LocalDate.parse(date,formatter).getMonthValue()
}
%>

raw = load '$taxi' using PigStorage(',',skipEmpty=true) as
  (vendor_id:chararray,pickup_datetime:chararray,dropoff_datetime:chararray,passenger_count:chararray,
  trip_distance:chararray, pickup_longitude:chararray,pickup_latitude:chararray,rate_code:chararray,
  store_and_fwd_flag:chararray,dropoff_longitude:chararray,dropoff_latitude:chararray,payment_type:chararray,
  fare_amount:chararray,surcharge:chararray,mta_tax:chararray,tip_amount:chararray,tolls_amount:chararray,total_amount:chararray);

noHeader = filter raw by not STARTSWITH(lower(vendor_id),"vendor");
month_tip = FOREACH noHeader GENERATE dateToMonth(pickup_datetime) as month:int, (double)tip_amount as tip

grp = GROUP month_tip by month;
avg = FOREACH grp GENERATE group, AVG(month_tip.tip);
dump avg mute;
