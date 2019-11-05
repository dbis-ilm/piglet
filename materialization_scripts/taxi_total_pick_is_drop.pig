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

noHeader = filter raw by not STARTSWITH(LOWER(vendor_id),"vendor") and pickup_longitude != "" and pickup_latitude != ""
                                                                     and dropoff_longitude != "" and dropoff_latitude != ""
                                                                     and total_amount != "";

month_total = FOREACH noHeader GENERATE geometry("POINT("+ pickup_latitude +" "+ pickup_longitude +")") as pickuploc,
                                        geometry("POINT("+ dropoff_latitude +" "+ dropoff_longitude +")") as dropoffloc,
                                        (double)total_amount as total;

allBlocks = load '$blocks' using PigStorage(';') as (blockid: int, wkt: chararray);
blocks = FOREACH allBlocks GENERATE geometry(wkt) as blockbounds, blockid;

pickup = SPATIAL_JOIN month_total, blocks ON CONTAINEDBY using index RTree(order=10);
pickup_block = FOREACH pickup GENERATE dropoffloc, blockid as pickupBlock, total;

allBlocks1 = load '$blocks' using PigStorage(';') as (blockid: int, wkt: chararray);
blocks1 = FOREACH allBlocks1 GENERATE geometry(wkt) as blockbounds, blockid;

pickupdropoff = SPATIAL_JOIN pickup_block, blocks1 ON CONTAINEDBY using index RTree(order=10);
pdt = FOREACH pickupdropoff GENERATE pickupBlock, blockid as dropoffBlock, total;

sameBlock = FILTER pdt BY pickupBlock == dropoffBlock;

grp = GROUP sameBlock by pickupBlock;
theMax = FOREACH grp GENERATE group, MAX(sameBlock.total) as maxTotal;

ordered = ORDER theMax BY maxTotal DESC;

dump ordered;
