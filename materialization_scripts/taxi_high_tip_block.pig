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

noHeader = filter raw by not STARTSWITH(LOWER(vendor_id),"vendor") and dropoff_longitude != "" and dropoff_latitude != ""
                                                                   and total_amount != "" and tip_amount != "";

month_total = FOREACH noHeader GENERATE geometry("POINT("+ dropoff_latitude +" "+ dropoff_longitude +")") as dropoffloc,
                                        (double)total_amount as total, (double)tip_amount as tip;

allBlocks = load '$blocks' using PigStorage(';') as (blockid: int, wkt: chararray);
blocks = FOREACH allBlocks GENERATE geometry(wkt) as blockbounds, blockid;

dropoff = SPATIAL_JOIN month_total, blocks ON CONTAINEDBY using index RTree(order=10);
dropoff_block = FOREACH dropoff GENERATE blockid, total, tip;


grp = GROUP dropoff_block by blockid;
avgs = FOREACH grp GENERATE group as blockid,   AVG(dropoff_block.tip) * 100 / AVG(dropoff_block.total) as p:double ;

hightip = FILTER avgs BY p >= 20;

sorted = ORDER hightip BY p DESC;

DUMP sorted;
