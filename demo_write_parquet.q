to_parquet:`libparquet_writer 2:(`write_parquet;3);
// setup splayed partitioned kdb table
trade:`symbol`exch_time xasc 10000000#("SPFJSNB*";1#",") 0: `:trades.csv;
d1:2025.04.02;
trade1:update exch_time:d1+"n"$exch_time from trade;
.Q.dd[`:db;d1,`trade`] set .Q.en[`:db;trade1];
\l db

/
in_mem:count[trade]#select from trade where date=d1;
to_parquet[in_mem;`$"/home/ak/kdb-parquet/build/trade.parquet";()]; // flat file sorted
to_parquet[in_mem;`$"/home/ak/kdb-parquet/build/trade_date";`date]; // single partition sorted
to_parquet[in_mem;`$"/home/ak/kdb-parquet/build/trade_date_sym";`date`symbol]; // multi partition sorted

// leverage mmapped columns
on_dsk:select from trade where date=d1;
to_parquet[on_dsk;`$"/home/ak/kdb-parquet/build/trade.parquet";()]; // flat file sorted
to_parquet[on_dsk;`$"/home/ak/kdb-parquet/build/trade_date";`date]; // single partition sorted
to_parquet[on_dsk;`$"/home/ak/kdb-parquet/build/trade_date_sym";`date`symbol]; // multi partition sorted
\
exit 0;
