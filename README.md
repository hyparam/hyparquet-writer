# Hyparquet Writer

![hyparquet writer parakeet](hyparquet-writer.jpg)

[![npm](https://img.shields.io/npm/v/hyparquet-writer)](https://www.npmjs.com/package/hyparquet-writer)
[![minzipped](https://img.shields.io/bundlephobia/minzip/hyparquet-writer)](https://www.npmjs.com/package/hyparquet-writer)
[![workflow status](https://github.com/hyparam/hyparquet-writer/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet-writer/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-96-darkred)
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

Note: if `type` is not provided, the type will be guessed from the data. The supported types are a superset of the parquet types:

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

More types are supported but require defining the `schema` explicitly. See the [advanced usage](#advanced-usage) section for more details.

### Node.js Write to Local Parquet File

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

Options can be passed to `parquetWrite` to adjust parquet file writing behavior:

 - `writer`: a generic writer object
 - `schema`: parquet schema object (optional)
 - `compressed`: use snappy compression (default true)
 - `statistics`: write column statistics (default true)
 - `rowGroupSize`: number of rows in each row group (default 100000)
 - `kvMetadata`: extra key-value metadata to be stored in the parquet footer

```javascript
import { ByteWriter, parquetWrite } from 'hyparquet-writer'

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
  compressed: false,
  statistics: false,
  rowGroupSize: 1000,
  kvMetadata: [
    { key: 'key1', value: 'value1' },
    { key: 'key2', value: 'value2' },
  ],
})
const arrayBuffer = writer.getBuffer()
```

### Types

Parquet requires an explicit schema to be defined. You can provide schema information in three ways:

1. **Type**: You can provide a `type` in the `columnData` elements, the type will be used as the schema type.
2. **Schema**: You can provide a `schema` parameter that explicitly defines the parquet schema. The schema should be an array of `SchemaElement` objects (see [parquet-format](https://github.com/apache/parquet-format)), each containing the following properties:
   - `name`: column name
   - `type`: parquet type
   - `num_children`: number children in parquet nested schema (optional)
   - `converted_type`: parquet converted type (optional)
   - `logical_type`: parquet logical type (optional)
   - `repetition_type`: parquet repetition type (optional)
   - `type_length`: length for `FIXED_LENGTH_BYTE_ARRAY` type (optional)
   - `scale`: the scale factor for `DECIMAL` converted types (optional)
   - `precision`: the precision for `DECIMAL` converted types (optional)
   - `field_id`: the field id for the column (optional)
3. **Auto-detect**: If you provide no type or schema, the type will be auto-detected from the data. However, it is recommended that you provide type information when possible. (zero rows would throw an exception, floats might be typed as int, etc)

Most converted types will be auto-detected if you just provide data with no types. However, it is still recommended that you provide type information when possible. (zero rows would throw an exception, floats might be typed as int, etc)

#### Schema Overrides

You can use mostly automatic schema detection, but override the schema for specific columns. This is useful if most of the column types can be automatically determined, but you want to use a specific schema element for one particular element.

```javascript
import { parquetWrite, schemaFromColumnData } from 'hyparquet-writer'

const columnData = [
  { name: 'unsigned_int', data: [1000000, 2000000] },
]
parquetWrite({
  columnData,
  // override schema for uint column
  schema: schemaFromColumnData(columnData, {
    unsigned_int: {
      type: 'INT32',
      converted_type: 'UINT_32',
    },
  }),
})
```

## References

 - https://github.com/hyparam/hyparquet
 - https://github.com/hyparam/hyparquet-compressors
 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
