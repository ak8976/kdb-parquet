\l parquet.q
trade:10000000#("SPFJSNB*";1#",") 0: `:trades.csv;
d1:2025.04.02;
trade1:update date:d1, exch_time:d1+"n"$exch_time from trade;
d2:2025.04.03;
trade2:update date:d2,exch_time:d2+"n"$exch_time from trade;
trade:`date`symbol`exch_time xasc trade1,trade2

to_parquet[trade;`trade.parquet;()]; // flat file sorted
to_parquet[trade;`trade_date;`date]; // single partition sorted
to_parquet[trade;`trade_date_sym;`date`symbol]; // multi partition sorted
exit 0;
