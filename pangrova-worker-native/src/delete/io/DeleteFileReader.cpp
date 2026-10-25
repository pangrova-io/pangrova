// DeleteFileReader: reads Iceberg v2/v3 delete files (Parquet) and Puffin
// deletion vectors. Primary path uses Velox native DWIO readers for
// zero-conversion integration; falls back to Arrow when Velox DWIO is
// unavailable at compile time.
#include "delete/io/DeleteFileReader.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include "observability/PangrovaLog.h"
#include "split/PathUtils.h"

namespace pangrova::delete_filter {

// ============================================================
// Path 1: Velox native DWIO reader (preferred)
// ============================================================
#if PANGROVA_HAS_VELOX_DELETE_READER

namespace {

// Process-level memory pool for delete file reading.
// Delete files are small (typically < 10 MB) and read infrequently,
// so a shared leaf pool is acceptable.
facebook::velox::memory::MemoryPool& deleteReaderPool() {
  static auto pool =
      facebook::velox::memory::memoryManager()->addLeafPool(
          "delete_file_reader");
  return *pool;
}

// Resolve Velox file format — delete files are always Parquet in Iceberg.
facebook::velox::dwio::common::FileFormat resolveDeleteFileFormat(
    const std::string& path) {
  if (path.size() >= 4 &&
      path.compare(path.size() - 4, 4, ".orc") == 0) {
    return facebook::velox::dwio::common::FileFormat::DWRF;
  }
  return facebook::velox::dwio::common::FileFormat::PARQUET;
}

std::shared_ptr<facebook::velox::ReadFile> openDeleteFile(
    const std::string& path) {
  const auto normalized = split::normalizeToS3Uri(
      split::stripFilePrefix(path));
  auto file_system = facebook::velox::filesystems::getFileSystem(
      normalized, nullptr);
  if (!file_system) {
    throw std::runtime_error(
        "No registered filesystem for delete file: " + normalized);
  }
  return file_system->openFileForRead(normalized);
}

}  // namespace

std::vector<int64_t> DeleteFileReader::readPositions(
    const std::string& deleteFilePath,
    const std::string& dataFilePath) {
  auto& pool = deleteReaderPool();
  auto format = resolveDeleteFileFormat(deleteFilePath);

  // Open the delete file with Velox native reader.
  facebook::velox::dwio::common::ReaderOptions readerOpts(&pool);
  readerOpts.setFileFormat(format);

  std::unique_ptr<facebook::velox::dwio::common::Reader> reader;
  try {
    reader =
        facebook::velox::dwio::common::getReaderFactory(format)
            ->createReader(
                std::make_unique<facebook::velox::dwio::common::BufferedInput>(
                    openDeleteFile(deleteFilePath),
                    pool),
                readerOpts);
  } catch (const std::exception& e) {
    throw std::runtime_error(
        "Cannot open position delete file: path=" + deleteFilePath +
        ", error=" + e.what());
  }

  // Position delete schema: (file_path: VARCHAR, pos: BIGINT).
  auto outputType = facebook::velox::ROW(
      {"file_path", "pos"},
      {facebook::velox::VARCHAR(), facebook::velox::BIGINT()});

  facebook::velox::dwio::common::RowReaderOptions rowOpts;
  rowOpts.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          outputType));

  auto scanSpec =
      std::make_shared<facebook::velox::common::ScanSpec>("root");
  scanSpec->addField("file_path", 0);
  scanSpec->addField("pos", 1);
  rowOpts.setScanSpec(scanSpec);

  auto rowReader = reader->createRowReader(rowOpts);

  // Read batches and filter by dataFilePath.
  std::vector<int64_t> positions;
  constexpr uint64_t kBatchSize = 4096;

  while (true) {
    auto result =
        facebook::velox::BaseVector::create(outputType, 0, &pool);
    uint64_t rowsRead = rowReader->next(kBatchSize, result);
    if (rowsRead == 0) break;

    auto rowVector =
        std::dynamic_pointer_cast<facebook::velox::RowVector>(result);
    if (!rowVector) {
      throw std::runtime_error("Delete reader returned a non-row batch: " + deleteFilePath);
    }
    // Parquet may return lazy, constant or dictionary children. Typed flat
    // access is valid only after materializing those encodings.
    for (auto& child : rowVector->children()) {
      facebook::velox::BaseVector::flattenVector(child);
    }

    auto* pathVector =
        rowVector->childAt(0)->asFlatVector<facebook::velox::StringView>();
    auto* posVector =
        rowVector->childAt(1)->asFlatVector<int64_t>();

    if (!pathVector || !posVector) {
      throw std::runtime_error("Invalid position delete schema: " + deleteFilePath);
    }

    for (facebook::velox::vector_size_t i = 0;
         i < rowVector->size(); ++i) {
      if (pathVector->isNullAt(i) || posVector->isNullAt(i)) continue;

      auto filePath = pathVector->valueAt(i);
      // Match against dataFilePath. Empty dataFilePath means "all files".
      if (dataFilePath.empty() ||
          std::string_view(filePath.data(), filePath.size()) ==
              dataFilePath) {
        positions.push_back(posVector->valueAt(i));
      }
    }
  }

  return positions;
}

std::unordered_set<std::string> DeleteFileReader::readEqualityKeys(
    const std::string& deleteFilePath,
    const std::vector<std::string>& equalityColumns) {
  if (equalityColumns.empty()) {
    return {};
  }

  // Pre-check file existence to avoid Velox VELOX_CHECK abort on missing file.
  auto& pool = deleteReaderPool();
  auto format = resolveDeleteFileFormat(deleteFilePath);

  facebook::velox::dwio::common::ReaderOptions readerOpts(&pool);
  readerOpts.setFileFormat(format);

  std::unique_ptr<facebook::velox::dwio::common::Reader> reader;
  try {
    reader =
        facebook::velox::dwio::common::getReaderFactory(format)
            ->createReader(
                std::make_unique<facebook::velox::dwio::common::BufferedInput>(
                    openDeleteFile(deleteFilePath),
                    pool),
                readerOpts);
  } catch (const std::exception& e) {
    throw std::runtime_error(
        "Cannot open equality delete file: path=" + deleteFilePath +
        ", error=" + e.what());
  }

  // P1-TC-14: Read equality columns with actual Parquet types instead of
  // forcing VARCHAR. The Velox DWIO reader infers column types from the
  // Parquet schema. We read with inferred types and serialize values to
  // string for the equality key set. This avoids type mismatch when the
  // Parquet file stores non-string columns (INT, BIGINT, etc.).
  //
  // Build output type from equality columns — use the reader's schema
  // to resolve actual types. Fall back to VARCHAR if column not found.
  std::vector<std::string> names(equalityColumns);
  std::vector<facebook::velox::TypePtr> types;
  types.reserve(equalityColumns.size());

  // First, peek at the file schema to get actual column types.
  auto fileSchema = reader->typeWithId();
  std::unordered_map<std::string, facebook::velox::TypePtr> schemaTypes;
  if (fileSchema && fileSchema->type()->isRow()) {
    auto rowType = std::dynamic_pointer_cast<const facebook::velox::RowType>(
        fileSchema->type());
    if (rowType) {
      for (size_t i = 0; i < rowType->size(); ++i) {
        schemaTypes[rowType->nameOf(i)] = rowType->childAt(i);
      }
    }
  }

  for (const auto& colName : equalityColumns) {
    auto it = schemaTypes.find(colName);
    if (it != schemaTypes.end()) {
      types.push_back(it->second);
    } else {
      // Fallback to VARCHAR if column not found in schema.
      types.push_back(facebook::velox::VARCHAR());
    }
  }
  auto outputType = facebook::velox::ROW(std::move(names), std::move(types));

  facebook::velox::dwio::common::RowReaderOptions rowOpts;
  rowOpts.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          outputType));

  auto scanSpec =
      std::make_shared<facebook::velox::common::ScanSpec>("root");
  for (facebook::velox::column_index_t i = 0;
       i < static_cast<facebook::velox::column_index_t>(
               equalityColumns.size());
       ++i) {
    scanSpec->addField(equalityColumns[i], i);
  }
  rowOpts.setScanSpec(scanSpec);

  auto rowReader = reader->createRowReader(rowOpts);

  // Read batches and serialize each row as "col1|col2|..." key.
  std::unordered_set<std::string> keys;
  constexpr uint64_t kBatchSize = 4096;

  while (true) {
    auto result =
        facebook::velox::BaseVector::create(outputType, 0, &pool);
    uint64_t rowsRead = rowReader->next(kBatchSize, result);
    if (rowsRead == 0) break;

    auto rowVector =
        std::dynamic_pointer_cast<facebook::velox::RowVector>(result);
    if (!rowVector) {
      throw std::runtime_error("Delete reader returned a non-row batch: " + deleteFilePath);
    }
    // Parquet may return lazy, constant or dictionary children. Typed flat
    // access is valid only after materializing those encodings.
    for (auto& child : rowVector->children()) {
      facebook::velox::BaseVector::flattenVector(child);
    }

    for (facebook::velox::vector_size_t row = 0;
         row < rowVector->size(); ++row) {
      std::string key;
      // P0-TC-1: Use null byte separator consistent with EqualityDeleteFilter::apply().
      for (size_t col = 0; col < equalityColumns.size(); ++col) {
        if (col > 0) key += '\0';
        auto child = rowVector->childAt(col);
        if (child->isNullAt(row)) {
          key += "__NULL__";
        } else {
          // P1-TC-14: Serialize value based on actual column type.
          auto kind = child->typeKind();
          switch (kind) {
            case facebook::velox::TypeKind::VARCHAR:
            case facebook::velox::TypeKind::VARBINARY: {
              auto* strVector =
                  child->asFlatVector<facebook::velox::StringView>();
              if (strVector) {
                auto sv = strVector->valueAt(row);
                key.append(sv.data(), sv.size());
              }
              break;
            }
            case facebook::velox::TypeKind::BIGINT: {
              auto val = child->asFlatVector<int64_t>()->valueAt(row);
              key += std::to_string(val);
              break;
            }
            case facebook::velox::TypeKind::INTEGER: {
              auto val = child->asFlatVector<int32_t>()->valueAt(row);
              key += std::to_string(val);
              break;
            }
            case facebook::velox::TypeKind::SMALLINT: {
              auto val = child->asFlatVector<int16_t>()->valueAt(row);
              key += std::to_string(val);
              break;
            }
            case facebook::velox::TypeKind::TINYINT: {
              auto val = child->asFlatVector<int8_t>()->valueAt(row);
              key += std::to_string(val);
              break;
            }
            case facebook::velox::TypeKind::BOOLEAN: {
              auto val = child->asFlatVector<bool>()->valueAt(row);
              key += val ? "true" : "false";
              break;
            }
            case facebook::velox::TypeKind::DOUBLE: {
              auto val = child->asFlatVector<double>()->valueAt(row);
              key += std::to_string(val);
              break;
            }
            case facebook::velox::TypeKind::REAL: {
              auto val = child->asFlatVector<float>()->valueAt(row);
              key += std::to_string(val);
              break;
            }
            default: {
              // Fallback: try as StringView (works if Velox cast to VARCHAR).
              auto* strVector =
                  child->asFlatVector<facebook::velox::StringView>();
              if (strVector) {
                auto sv = strVector->valueAt(row);
                key.append(sv.data(), sv.size());
              }
              break;
            }
          }
        }
      }
      keys.insert(std::move(key));
    }
  }

  return keys;
}

#endif  // PANGROVA_HAS_VELOX_DELETE_READER

// ============================================================
// Deletion Vector reader — shared across all paths.
// Reads raw Puffin bytes (Roaring bitmap), no Parquet involved.
// ============================================================
roaring::Roaring DeleteFileReader::readDeletionVector(
    const std::string& dvFilePath) {
  std::ifstream file(dvFilePath, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    throw std::runtime_error(
        "Cannot open deletion vector file: " + dvFilePath);
  }
  auto size = file.tellg();
  file.seekg(0, std::ios::beg);
  std::vector<char> buffer(size);
  if (!file.read(buffer.data(), size)) {
    throw std::runtime_error(
        "Cannot read deletion vector file: " + dvFilePath);
  }
  return roaring::Roaring::readSafe(buffer.data(), buffer.size());
}

}  // namespace pangrova::delete_filter
