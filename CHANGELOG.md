# Hyparquet-writer Changelog

## [0.15.1]
 - Omit undefined values from variant objects
 - Optimize variant writing performance

## [0.15.0]
 - Variant logical type with shredding support (#29)

## [0.14.0]
 - Async-capable `Writer` with `finish()` and `flush()` (#28)

## [0.13.1]
 - Fix geospatial bounding box on partial coordinates

## [0.12.3]
 - `Writer.getBytes` for zero copy
 - Dictionary improvements

## [0.12.2]
 - Fix `boundary_order` in column index
 - Fix `first_row_index` on repeated values (#27)
 - Fix invalid parquet on falsy geometry
 - Export `geojsonToWkb`
 - One-pass encode nested values
 - Simplify dremel normalization

## [0.12.1]
 - Fix date conversion to int (#26)
 - Fix crash when timestamp/date values aren't `Date` objects (#24)
 - Parquet default parameters (#22)

## [0.12.0]
 - Nested struct encoding (#21)
 - Default `rowGroupSize = [100, 1000, 10000]`
 - Skip page indexes when there is only one page

## [0.11.2]
 - Fix hyparquet dependency

## [0.11.1]
 - Write indexes at footer
 - Split options for `columnIndex` and `offsetIndex`
 - Fix column / page index alignment

## [0.11.0]
 - Page indexes (column index and offset index)
 - Custom compressors (#19)
 - Limit page size
 - Move `hyparquet` to dev dependencies

## [0.10.1]
 - Fix RLE encoding length (#18)

## [0.10.0]
 - `DELTA_BINARY_PACKED` encoding
 - `BYTE_STREAM_SPLIT` encoding
 - User-controllable encoding options
 - Rename `PageData`
 - Return `ColumnChunk` from `writeColumn`

## [0.9.1]
 - Geospatial stats (#13)
 - Fix JSON serialization of bigints

## [0.9.0]
 - Geospatial logical type (`GEOMETRY` / `GEOGRAPHY`) (#12)
 - `schemaOverrides` supports nested column types
 - Fix thrift encoding when delta > 15

## [0.8.0]
 - Dremel list encoding for repeated types
 - Refactor `ColumnEncoder` to pass around column info

## [0.7.0]
 - Default-only exports (#10)
 - Enable exports for default imports (#9)

## [0.6.1]
 - Handle optional JSON columns

## [0.6.0]
 - `rowGroupSize` can be an array
 - Update hyparquet internals

## [0.5.1]
 - `sideEffects: false` for better tree-shaking
 - Allow null columns to be auto-typed

## [0.5.0]
 - Export node entry by default for better Next.js support
 - Export types
 - Rename `ColumnData` to `ColumnSource`

## [0.4.0]
 - Refactor api to support arbitrary parquet schemas (#3)

## [0.3.5]
 - Float16 support
 - UUID and improved fixed-length byte array support
 - Throw for repeated types (not yet supported)

## [0.3.4]
 - RLE encoding for booleans
 - Check for safe integers
 - Pass `bitWidth` to `writeRleBitPackedHybrid` to avoid re-scanning
 - Round-trip tests

## [0.3.3]
 - Set dictionary threshold to 2

## [0.3.2]
 - Fix statistics writing for dates and decimals

## [0.3.1]
 - Don't write `file_path` (duckdb compatibility)

## [0.3.0]
 - Split out node.js exports

## [0.2.5]
 - Find bitwidth faster for large arrays
 - TPCH benchmark

## [0.2.4]
 - Fix offset handling for `fileWriter`

## [0.2.3]
 - Split out `writeDataPageV2`
 - Move helpers to `datapage.js`

## [0.2.2]
 - Fix `DATE` converted type
 - Fixed-length byte array decimals

## [0.2.1]
 - Support more `SchemaElement` options
 - Unconvert decimal type
 - Fix metadata thrift encoding

## [0.2.0]
 - `FileWriter`
 - Float support

## [0.1.7]
 - BYO `Writer`
 - Streaming writer

## [0.1.6]
 - `rowGroupSize` option
 - Write statistics
 - Type thrift
 - Move `convert` to `unconvert` and test it

## [0.1.5]
 - Use constants from hyparquet

## [0.1.4]
 - `key_value_metadata` option

## [0.1.3]
 - Allow specifying column type

## [0.1.2]
 - Dictionary encoding
 - Optional compression flag

## [0.1.1]
 - Snappy compression
 - Nullable columns
 - Choose best of RLE or bit-packed for hybrid encoding
 - Handle byte array vs string
 - Date and JSON types

## [0.1.0]
 - Initial release
