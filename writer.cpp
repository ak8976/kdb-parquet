#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <parquet/arrow/writer.h>
#include <set>
extern "C" {
#include "k.h"
}
using namespace arrow;
using namespace std;
#define APPEND_ARRAY(builder, arrow_type_expr)                                 \
  {                                                                            \
    shared_ptr<Array> array;                                                   \
    ARROW_RETURN_NOT_OK(builder.Finish(&array));                               \
    arrays.push_back(array);                                                   \
    fields.push_back(field(col_name, arrow_type_expr));                        \
  }
#define APPEND_VALUE(value, null_expr)                                         \
  {                                                                            \
    if (value == null_expr) {                                                  \
      ARROW_RETURN_NOT_OK(builder.AppendNull());                               \
    } else {                                                                   \
      ARROW_RETURN_NOT_OK(builder.Append(value));                              \
    }                                                                          \
  }
#define CHECK_STATUS(expr)                                                     \
  {                                                                            \
    status = expr;                                                             \
    if (!status.ok()) {                                                        \
      k_err = status.message();                                                \
      return krr((S)k_err.c_str());                                            \
    }                                                                          \
  }
bool is_null(const char* symbol) {
  return symbol[0] == '\0';
}
bool is_in(const char* symbol, K symbol_list) {
  for (size_t i = 0; i < symbol_list->n; ++i) {
    if (strcmp(symbol, kS(symbol_list)[i]) == 0) {
      return true;
    }
  }
  return false;
}
static const set<string> allowed_options = {"use_threads",  "enable_dict",
                                            "disable_dict", "chunk_size",
                                            "store_schema", "compression"};
Status kdb_to_arrow(shared_ptr<Table>& arrow_table, K table) {
  K col_names = kK(table->k)[0];
  K col_vectors = kK(table->k)[1];
  int n_rows = kK(col_vectors)[0]->n;
  vector<shared_ptr<Field>> fields;
  vector<shared_ptr<Array>> arrays;
  for (size_t c = 0; c < col_names->n; ++c) {
    string col_name = kS(col_names)[c];
    K col = kK(col_vectors)[c];
    bool is_enum = (col->t >= 20 && col->t <= 76);
    if (is_enum) {
      col = k(0, (S) "value", r1(col), (K)0); // de-enumerate enum list
      if (!col) return Status::Invalid("Failed to de-enumerate list");
    }
    switch (col->t) {
      case 0: { // mixed (could be string)
        for (size_t i = 0; i < n_rows; ++i) {
          if (kK(col)[i]->t != KC) {
            return Status::Invalid(
                "Unsupported general list structure (not string list)");
          }
        }
        StringBuilder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          K str_k = kK(col)[i];
          string s((S)kC(str_k), str_k->n);
          ARROW_RETURN_NOT_OK(builder.Append(s));
        }
        APPEND_ARRAY(builder, utf8());
        break;
      }
      case 77: { // anymap (could be string)
        for (size_t i = 0; i < n_rows; ++i) {
          K item = vi(col, i);
          if (item->t != KC) {
            r0(item);
            return Status::Invalid(
                "Unsupported anymap structure (not string list)");
          }
          r0(item);
        }
        StringBuilder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          K item = vi(col, i);
          string s((S)kC(item), item->n);
          r0(item);
          ARROW_RETURN_NOT_OK(builder.Append(s));
        }
        APPEND_ARRAY(builder, utf8());
        break;
      }
      case KB: { // boolean
        BooleanBuilder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          ARROW_RETURN_NOT_OK(builder.Append(bool(kG(col)[i])));
        }
        APPEND_ARRAY(builder, boolean());
        break;
      }
      case KH: { // short
        Int16Builder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          APPEND_VALUE(kH(col)[i], nh);
        }
        APPEND_ARRAY(builder, int16());
        break;
      }
      case KI: { // int
        Int32Builder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          APPEND_VALUE(kI(col)[i], ni);
        }
        APPEND_ARRAY(builder, int32());
        break;
      }
      case KJ: { // long
        Int64Builder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          APPEND_VALUE(kJ(col)[i], nj);
        }
        APPEND_ARRAY(builder, int64());
        break;
      }
      case KE: { // real
        FloatBuilder builder;
        E value;
        for (size_t i = 0; i < n_rows; ++i) {
          value = kE(col)[i];
          if (isnan(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(value));
          }
        }
        APPEND_ARRAY(builder, float32());
        break;
      }
      case KF: { // float
        DoubleBuilder builder;
        for (size_t i = 0; i < n_rows; ++i) {
          APPEND_VALUE(kF(col)[i], nf);
        }
        APPEND_ARRAY(builder, float64());
        break;
      }
      case KD: { // date
        Date32Builder builder;
        I value;
        constexpr int kdb_epoch_offset = 10957;
        for (size_t i = 0; i < n_rows; ++i) {
          value = kI(col)[i];
          if (value == ni) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(value + kdb_epoch_offset));
          }
        }
        APPEND_ARRAY(builder, date32());
        break;
      }
      case KS: { // symbol
        StringBuilder builder;
        S value;
        for (size_t i = 0; i < n_rows; ++i) {
          value = kS(col)[i];
          if (is_null(value)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(value));
          }
        }
        APPEND_ARRAY(builder, utf8());
        break;
      }
      case KP: { // timestamp
        TimestampBuilder builder(timestamp(TimeUnit::NANO),
                                 default_memory_pool());
        constexpr long long kdb_epoch_offset = 946684800000000000LL;
        J value;
        for (size_t i = 0; i < n_rows; ++i) {
          value = kJ(col)[i];
          if (value == nj) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(value + kdb_epoch_offset));
          }
        }
        APPEND_ARRAY(builder, timestamp(TimeUnit::NANO));
        break;
      }
      case KN: { // timespan
        Time64Builder builder(time64(TimeUnit::NANO), default_memory_pool());
        for (size_t i = 0; i < n_rows; ++i) {
          APPEND_VALUE(kJ(col)[i], nj);
        }
        APPEND_ARRAY(builder, time64(TimeUnit::NANO));
        break;
      }
      case KT: { // time
        Time32Builder builder(time32(TimeUnit::MILLI), default_memory_pool());
        for (size_t i = 0; i < n_rows; ++i) {
          APPEND_VALUE(kI(col)[i], ni);
        }
        APPEND_ARRAY(builder, time32(TimeUnit::NANO));
        break;
      }
      default:
        return Status::Invalid("Unsupported column type: " +
                               to_string(int(col->t)));
    }
    if (is_enum) r0(col);
  }
  arrow_table = Table::Make(make_shared<Schema>(fields), arrays);
  return Status::OK();
}
Status
set_writer_properties(K& opts,
                      shared_ptr<parquet::ArrowWriterProperties>& arrow_props,
                      std::shared_ptr<parquet::WriterProperties>& parq_props) {
  if (opts->n) {
    auto arrow_writer_props = new parquet::ArrowWriterProperties::Builder();
    auto parq_writer_props = new parquet::WriterProperties::Builder();
    K keys = kK(opts)[0];
    K vals = kK(opts)[1];
    for (size_t i = 0; i < keys->n; ++i) {
      string opt(kS(keys)[i]);
      if (allowed_options.find(opt) == allowed_options.end()) {
        return Status::Invalid("Invalid option: " + opt);
      }
      if (allowed_options.find(opt) != allowed_options.end()) {
        // parquet writer properties
        if (opt == "compression") {
          string codec;
          if (kK(vals)[i]->t == -KS) {
            codec = kK(vals)[i]->s;
          } else if (vals->t == KS) {
            codec = kS(vals)[i];
          }
          if (codec == "snappy")
            parq_writer_props->compression(Compression::SNAPPY);
          else if (codec == "zstd")
            parq_writer_props->compression(Compression::ZSTD);
          else if (codec == "gzip")
            parq_writer_props->compression(Compression::GZIP);
          else
            return Status::Invalid("Unsupported compression: " + codec);
        }
        if (opt == "enable_dict") {
          if (kK(vals)[i]->t == KS) {
            for (size_t j = 0; j < kK(vals)[i]->n; ++j) {
              parq_writer_props->enable_dictionary(kS(kK(vals)[i])[j]);
            }
          } else if (vals->t == KS) {
            parq_writer_props->enable_dictionary(kS(vals)[i]);
          } else if (vals->t == KB) {
            if (static_cast<bool>(kG(vals)[i]))
              parq_writer_props->enable_dictionary();
          } else if (kK(vals)[i]->t == -KB) {
            if (static_cast<bool>(kK(vals)[i]->g))
              parq_writer_props->enable_dictionary();
          }
        }
        if (opt == "disable_dict") {
          if (kK(vals)[i]->t == KS) {
            for (size_t j = 0; j < kK(vals)[i]->n; ++j) {
              parq_writer_props->disable_dictionary(kS(kK(vals)[i])[j]);
            }
          } else if (vals->t == KS) {
            parq_writer_props->disable_dictionary(kS(vals)[i]);
          } else if (vals->t == KB) {
            if (static_cast<bool>(kG(vals)[i]))
              parq_writer_props->disable_dictionary();
          } else if (kK(vals)[i]->t == -KB) {
            if (static_cast<bool>(kK(vals)[i]->g))
              parq_writer_props->disable_dictionary();
          }
        }
        if (opt == "chunk_size") {
          if (kK(vals)[i]->t == -KJ) {
            parq_writer_props->max_row_group_length(kK(vals)[i]->j);
          } else if (vals->t == KJ) {
            parq_writer_props->max_row_group_length(kJ(vals)[i]);
          }
        }
        // arrow writer properties
        if (opt == "use_threads") {
          if (kK(vals)[i]->t == -KB) {
            arrow_writer_props->set_use_threads(
                static_cast<bool>(kK(vals)[i]->g));
          } else if (vals->t == KB) {
            arrow_writer_props->set_use_threads(static_cast<bool>(kG(vals)[i]));
          }
        }
        if (opt == "store_schema") {
          if (kK(vals)[i]->t == -KB && static_cast<bool>(kK(vals)[i]->g)) {
            arrow_writer_props->store_schema();
          } else if (vals->t == KB && static_cast<bool>(kG(vals)[i])) {
            arrow_writer_props->store_schema();
          }
        }
      }
    }
    arrow_props = arrow_writer_props->build();
    parq_props = parq_writer_props->build();
  }
  return Status::OK();
}
Status set_write_options(dataset::FileSystemDatasetWriteOptions& write_options,
                         shared_ptr<Table>& arrow_table,
                         shared_ptr<fs::FileSystem>& fs,
                         vector<string>& par_cols, filesystem::path& path,
                         K& opts) {
  try {
    vector<shared_ptr<Field>> par_fields;
    for (const string& col_name : par_cols) {
      par_fields.push_back(arrow_table->schema()->GetFieldByName(col_name));
    }
    auto partitioning =
        make_shared<dataset::HivePartitioning>(make_shared<Schema>(par_fields));
    write_options.partitioning = partitioning;
    write_options.file_write_options =
        make_shared<dataset::ParquetFileFormat>()->DefaultWriteOptions();
    write_options.filesystem = fs;
    write_options.base_dir = path.string();
    write_options.basename_template = "part{i}.parquet";
    write_options.existing_data_behavior =
        dataset::ExistingDataBehavior::kOverwriteOrIgnore;
    auto options =
        internal::checked_pointer_cast<dataset::ParquetFileWriteOptions>(
            write_options.file_write_options);
    Status st = set_writer_properties(opts, options->arrow_writer_properties,
                                      options->writer_properties);
    if (!st.ok()) {
      return Status::Invalid(st.message());
    }
  } catch (const exception& e) {
    return Status::Invalid(e.what());
  }
  return Status::OK();
}
extern "C" K write_parquet(K table, K path, K k_par_cols, K opts) {
  if (table->t != XT) {
    return krr((S) "not a table");
  }
  if (path->t != -KS) {
    return krr((S) "Path not a symbol");
  }
  if (!(k_par_cols->t == -KS || k_par_cols->t == KS || k_par_cols->n == 0)) {
    return krr((S) "Partition column(s) must be symbol/symbol list");
  }
  if (opts->t != XD) {
    return krr((S) "opts not a dictionary");
  }
  vector<string> par_cols;
  if (k_par_cols->t == -KS) {
    if (!is_null(k_par_cols->s)) {
      if (!is_in(k_par_cols->s, kK(table->k)[0])) {
        return krr((S) "Partition column does not exist");
      }
      par_cols.emplace_back(k_par_cols->s);
    }
  } else if (k_par_cols->t == KS) {
    for (size_t i = 0; i < k_par_cols->n; ++i) {
      if (!is_null(kS(k_par_cols)[i])) {
        if (!is_in(kS(k_par_cols)[i], kK(table->k)[0])) {
          return krr((S) "Partition column does not exist");
        }
        par_cols.emplace_back(kS(k_par_cols)[i]);
      }
    }
  }
  auto abs_path = filesystem::absolute(filesystem::path(path->s));
  static string k_err;
  Status status;
  shared_ptr<Table> arrow_table;
  CHECK_STATUS(kdb_to_arrow(arrow_table, table));
  try {
    if (par_cols.empty()) {
      // No partition columns, save as flat file
      shared_ptr<io::FileOutputStream> outfile;
      CHECK_STATUS(
          io::FileOutputStream::Open(abs_path.string()).Value(&outfile));
      std::shared_ptr<parquet::ArrowWriterProperties> arrow_props;
      std::shared_ptr<parquet::WriterProperties> parq_props =
          parquet::default_writer_properties();
      CHECK_STATUS(set_writer_properties(opts, arrow_props, parq_props));
      CHECK_STATUS(parquet::arrow::WriteTable(
          *arrow_table, default_memory_pool(), outfile,
          parquet::DEFAULT_MAX_ROW_GROUP_LENGTH, parq_props, arrow_props));
    } else {
      // Save as Hive Partitioned table
      auto write_dataset = make_shared<TableBatchReader>(arrow_table);
      auto write_scanner_builder =
          dataset::ScannerBuilder::FromRecordBatchReader(write_dataset);
      shared_ptr<dataset::Scanner> write_scanner;
      CHECK_STATUS(write_scanner_builder->Finish().Value(&write_scanner));
      shared_ptr<fs::FileSystem> fs;
      CHECK_STATUS(fs::FileSystemFromUriOrPath(abs_path.parent_path().string())
                       .Value(&fs));
      dataset::FileSystemDatasetWriteOptions write_options;
      CHECK_STATUS(set_write_options(write_options, arrow_table, fs, par_cols,
                                     abs_path, opts));
      CHECK_STATUS(
          dataset::FileSystemDataset::Write(write_options, write_scanner));
    }
  } catch (const exception& e) {
    k_err = e.what();
    return krr((S)k_err.c_str());
  }
  return (K)0;
}
