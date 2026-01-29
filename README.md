# Hyparquet Writer

![hyparquet writer parakeet](hyparquet-writer.jpg)

[![npm](https://img.shields.io/npm/v/hyparquet-writer)](https://www.npmjs.com/package/hyparquet-writer)
[![minzipped](https://img.shields.io/bundlephobia/minzip/hyparquet-writer)](https://www.npmjs.com/package/hyparquet-writer)
[![workflow status](https://github.com/hyparam/hyparquet-writer/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet-writer/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-95-darkred)
[![dependencies](https://img.shields.io/badge/Dependencies-1-blueviolet)](https://www.npmjs.com/package/hyparquet-writer?activeTab=dependencies)

Hyparquet Writer is a JavaScript library for writing [Apache Parquet](https://parquet.apache.org) files. It is designed to be lightweight, fast and store data very efficiently. It is a companion to the [hyparquet](https://github.com/hyparam/hyparquet) library, which is a JavaScript library for reading parquet files.

## Quick Start

To write a parquet file to an `ArrayBuffer` use `parquetWriteBuffer` with argument `columnData`. Each column in `columnData` should contain:

- `name`: the column name
- `data`: an array of same-type values
- `type`: the parquet schema type (optional)

```javascript
import { parquetWriteBuffer } from 'hyparquet-writer'

const arrayBuffer = parquetWriteBuffer({
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'STRING' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
})
```

Note: if `type` is not provided, the type will be guessed from the data. The supported `BasicType` are a superset of the parquet primitive types:

| Basic Type | Equivalent Schema Element |
|------|----------------|
| `BOOLEAN` | `{ type: 'BOOLEAN' }` |
| `INT32` | `{ type: 'INT32' }` |
| `INT64` | `{ type: 'INT64' }` |
| `FLOAT` | `{ type: 'FLOAT' }` |
| `DOUBLE` | `{ type: 'DOUBLE' }` |
| `BYTE_ARRAY` | `{ type: 'BYTE_ARRAY' }` |
| `STRING` | `{ type: 'BYTE_ARRAY', converted_type: 'UTF8' }` |
| `JSON` | `{ type: 'BYTE_ARRAY', converted_type: 'JSON' }` |
| `TIMESTAMP` | `{ type: 'INT64', converted_type: 'TIMESTAMP_MILLIS' }` |
| `UUID` | `{ type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16, logical_type: { type: 'UUID' } }` |
| `FLOAT16` | `{ type: 'FIXED_LEN_BYTE_ARRAY', type_length: 2, logical_type: { type: 'FLOAT16' } }` |
| `GEOMETRY` | `{ type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' } }` |
| `GEOGRAPHY` | `{ type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' } }` |

More types are supported but require defining the `schema` explicitly. See the [advanced usage](#advanced-usage) section for more details.

### Write to Local Parquet File (nodejs)

To write a local parquet file in node.js use `parquetWriteFile` with arguments `filename` and `columnData`:

```javascript
const { parquetWriteFile } = await import('hyparquet-writer')

parquetWriteFile({
  filename: 'example.parquet',
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'STRING' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
})
```

Note: hyparquet-writer is published as an ES module, so dynamic `import()` may be required on the command line.

## Advanced Usage

By default, hyparquet-writer generates parquet files that are optimized for large text datasets and fast previews. Parquet file parameters can be configured via options:

```typescript
interface ParquetWriteOptions {
  writer: Writer // generic writer
  columnData: ColumnSource[]
  schema?: SchemaElement[] // explicit parquet schema
  codec?: CompressionCodec // compression codec (default 'SNAPPY')
  compressors?: Compressors // custom compressors (default includes snappy)
  statistics?: boolean // enable column statistics (default true)
  pageSize?: number // target page size in bytes (default 1 mb)
  rowGroupSize?: number | number[] // target row group size in rows (default [1000, 100000])
  kvMetadata?: { key: string; value?: string }[] // extra key-value metadata
}
```

Note: `rowGroupSize` can be either constant or an array of row group sizes, with the last size repeating. The default `[1000, 100000]` means the first row group will have 1000 rows, and all subsequent row groups will have 100,000 rows. This is optimized for fast previews of large datasets.

Per-column options:

```typescript
interface ColumnSource {
  name: string
  data: DecodedArray
  type?: BasicType
  nullable?: boolean // allow nulls (default true)
  encoding?: Encoding // parquet encoding (PLAIN, RLE, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT, etc)
  columnIndex?: boolean // enable page-level column index (default false)
  offsetIndex?: boolean // enable page-level offset index (default true)
}
```

Example:

```javascript
import { ByteWriter, parquetWrite } from 'hyparquet-writer'
import { snappyCompress } from 'hysnappy'

const writer = new ByteWriter()
parquetWrite({
  writer,
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'] },
    { name: 'age', data: [25, 30, 35] },
    { name: 'dob', data: [new Date(1000000), new Date(2000000), new Date(3000000)] },
  ],
  // explicit schema:
  schema: [
    { name: 'root', num_children: 3 },
    { name: 'name', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
    { name: 'age', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 4, converted_type: 'DECIMAL', scale: 2, precision: 4 },
    { name: 'dob', type: 'INT32', converted_type: 'DATE' },
  ],
  compressors: { SNAPPY: snappyCompresss }, // high performance wasm compressor
  statistics: false, // disable statistics
  rowGroupSize: 1000000, // large row groups
  kvMetadata: [
    { key: 'key1', value: 'value1' },
    { key: 'key2', value: 'value2' },
  ],
})
const arrayBuffer = writer.getBuffer()
```

## Column Types

Hyparquet-writer supports several ways to define the parquet schema. The simplest way is to provide basic types in the `columnData` elements.

If you don't provide types, the types will be auto-detected from the data. However, it is still recommended that you provide type information when possible. (zero rows would throw an exception, floats might be typed as int, etc)

### Explicit Schema

You can provide your own parquet schema of type `SchemaElement` (see [parquet-format](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)):

```typescript
import { ByteWriter, parquetWrite } from 'hyparquet-writer'

const writer = new ByteWriter()
parquetWrite({
  writer,
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'] },
    { name: 'age', data: [25, 30, 35] },
  ],
  // explicit schema:
  schema: [
    { name: 'root', num_children: 2 },
    { name: 'name', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' },
    { name: 'age', type: 'INT32', repetition_type: 'REQUIRED' },
  ],
})
```

### Schema Overrides

You can use mostly automatic schema detection, but override the schema for specific columns. This is useful if most of the column types can be automatically determined, but you want to use a specific schema element for one particular element.

```javascript
const { ByteWriter, parquetWrite, schemaFromColumnData } = await import("hyparquet-writer")

// one unsigned and one signed int column
const columnData = [
  { name: 'unsigned_int', data: [1000000, 2000000] },
  { name: 'signed_int', data: [1000000, 2000000] },
]
const writer = new ByteWriter()
parquetWrite({
  writer,
  columnData,
  // override schema for unsigned_int column
  schema: schemaFromColumnData({
    columnData,
    schemaOverrides: {
      unsigned_int: {
        name: 'unsigned_int',
        type: 'INT32',
        converted_type: 'UINT_32',
        repetition_type: 'REQUIRED',
      },
    },
  }),
})
```

## References

 - https://github.com/hyparam/hyparquet
 - https://github.com/hyparam/hyparquet-compressors
 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
