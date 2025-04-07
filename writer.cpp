#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <parquet/arrow/writer.h>
#include <unistd.h>
extern "C" {
#include "k.h"
}
using namespace arrow;
#define APPEND_ARRAY(builder, arrow_type_expr)                                 \
  {                                                                            \
    std::shared_ptr<Array> array;                                              \
    ARROW_RETURN_NOT_OK(builder.Finish(&array));                               \
    arrays.push_back(array);                                                   \
    fields.push_back(field(colname, arrow_type_expr));                         \
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
// Helper: Convert KDB table to Arrow table
Status kdb_to_arrow(std::shared_ptr<Table>& arrow_table, K table) {
  if (table->t != 98) {
    throw std::runtime_error("Not a kdb+ table");
  }
  K col_names = kK(table->k)[0];
  K col_vectors = kK(table->k)[1];
  int n_rows = kK(col_vectors)[0]->n;
  std::vector<std::shared_ptr<Field>> fields;
  std::vector<std::shared_ptr<Array>> arrays;
  for (size_t c = 0; c < col_names->n; ++c) {
    std::string colname = kS(col_names)[c];
    K col = kK(col_vectors)[c];
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
          std::string s((S)kC(str_k), str_k->n);
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
          if (std::isnan(value)) {
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
          if (value[0] == '\0') {
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
                               std::to_string(int(col->t)));
    }
  }
  arrow_table = Table::Make(std::make_shared<Schema>(fields), arrays);
  return arrow::Status::OK();
}

void set_write_options(dataset::FileSystemDatasetWriteOptions& write_options,
                       std::shared_ptr<Table>& arrow_table,
                       std::shared_ptr<fs::FileSystem>& fs,
                       std::vector<std::string>& par_cols,
                       std::string base_dir) {
  std::vector<std::shared_ptr<Field>> par_fields;
  for (const std::string& col_name : par_cols) {
    par_fields.push_back(arrow_table->schema()->GetFieldByName(col_name));
  }
  auto partitioning = std::make_shared<dataset::HivePartitioning>(
      std::make_shared<Schema>(par_fields));
  write_options.partitioning = partitioning;
  auto write_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  write_options.file_write_options = write_format->DefaultWriteOptions();
  write_options.filesystem = fs;
  write_options.base_dir = base_dir;
  write_options.basename_template = "part{i}.parquet";
  write_options.existing_data_behavior =
      dataset::ExistingDataBehavior::kOverwriteOrIgnore;
}
// Exported KDB foreign function
extern "C" K write_parquet(K table, K path, K k_par_cols) {
  if (table->t != 98) {
    return krr((S) "Not a table");
  }
  if (path->t != -11) {
    return krr((S) "Path not a symbol");
  }
  if (!(k_par_cols->t == -11 || k_par_cols->t == 11)) {
    return krr((S) "Partition column(s) must be symbol/symbol list");
  }
  std::vector<std::string> pq_par_cols;
  if (k_par_cols->t == -11) {
    if (k_par_cols->s[0] != '\0') {
      pq_par_cols.emplace_back(k_par_cols->s);
    }
  } else if (k_par_cols->t == 11) {
    for (size_t i = 0; i < k_par_cols->n; ++i) {
      if (kS(k_par_cols)[i][0] != '\0') {
        pq_par_cols.emplace_back(kS(k_par_cols)[i]);
      }
    }
  }
  auto abs_path = std::filesystem::absolute(std::filesystem::path(path->s));
  static std::string k_err;
  try {
    std::shared_ptr<Table> arrow_table;
    Status status;
    CHECK_STATUS(kdb_to_arrow(arrow_table, table));
    if (pq_par_cols.empty()) {
      // No partition columns, save as flat file
      std::shared_ptr<arrow::io::FileOutputStream> outfile;
      CHECK_STATUS(
          io::FileOutputStream::Open(abs_path.string()).Value(&outfile));
      CHECK_STATUS(parquet::arrow::WriteTable(*arrow_table,
                                              default_memory_pool(), outfile));
    } else {
      // Save as Hive Partitioned table
      auto write_dataset = std::make_shared<TableBatchReader>(arrow_table);
      auto write_scanner_builder =
          dataset::ScannerBuilder::FromRecordBatchReader(write_dataset);
      std::shared_ptr<arrow::dataset::Scanner> write_scanner;
      CHECK_STATUS(write_scanner_builder->Finish().Value(&write_scanner));
      std::shared_ptr<fs::FileSystem> fs;
      CHECK_STATUS(fs::FileSystemFromUriOrPath(abs_path.parent_path().string())
                       .Value(&fs));
      dataset::FileSystemDatasetWriteOptions write_options;
      set_write_options(write_options, arrow_table, fs, pq_par_cols,
                        abs_path.filename().string());
      CHECK_STATUS(
          dataset::FileSystemDataset::Write(write_options, write_scanner));
    }
  } catch (const std::exception& e) {
    k_err = e.what();
    return krr((S)k_err.c_str());
  }
  return (K)0;
}
