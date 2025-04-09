to_parquet:{[f;x;y;z]f[x;y;z];-1"Wrote ", string[count x], " rows to ",string y;}[`libparquet_writer 2:(`write_parquet;3)];
