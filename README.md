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
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'BYTE_ARRAY' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
})
```

Note: if `type` is not provided, the type will be guessed from the data. The supported parquet types are:

- `BOOLEAN`
- `INT32`
- `INT64`
- `FLOAT`
- `DOUBLE`
- `BYTE_ARRAY`
- `FIXED_LEN_BYTE_ARRAY`

Strings are represented in parquet as type `BYTE_ARRAY`.

### Node.js Write to Local Parquet File

To write a local parquet file in node.js use `parquetWriteFile` with arguments `filename` and `columnData`:

```javascript
const { parquetWriteFile } = await import('hyparquet-writer')

parquetWriteFile({
  filename: 'example.parquet',
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'BYTE_ARRAY' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
})
```

Note: hyparquet-writer is published as an ES module, so dynamic `import()` may be required on the command line.

## Advanced Usage

Options can be passed to `parquetWrite` to adjust parquet file writing behavior:

 - `writer`: a generic writer object
 - `compressed`: use snappy compression (default true)
 - `statistics`: write column statistics (default true)
 - `rowGroupSize`: number of rows in each row group (default 100000)
 - `kvMetadata`: extra key-value metadata to be stored in the parquet footer

```javascript
import { ByteWriter, parquetWrite } from 'hyparquet-writer'

const writer = new ByteWriter()
const arrayBuffer = parquetWrite({
  writer,
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'BYTE_ARRAY' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
  compressed: false,
  statistics: false,
  rowGroupSize: 1000,
  kvMetadata: [
    { key: 'key1', value: 'value1' },
    { key: 'key2', value: 'value2' },
  ],
})
```

### Converted Types

You can provide additional type hints by providing a `converted_type` to the `columnData` elements:

```javascript
parquetWrite({
  columnData: [
    {
      name: 'dates',
      data: [new Date(1000000), new Date(2000000)],
      type: 'INT64',
      converted_type: 'TIMESTAMP_MILLIS',
    },
    {
      name: 'json',
      data: [{ foo: 'bar' }, { baz: 3 }, 'imastring'],
      type: 'BYTE_ARRAY',
      converted_type: 'JSON',
    },
  ]
})
```

Most converted types will be auto-detected if you just provide data with no types. However, it is still recommended that you provide type information when possible. (zero rows would throw an exception, floats might be typed as int, etc)

## References

 - https://github.com/hyparam/hyparquet
 - https://github.com/hyparam/hyparquet-compressors
 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
