# Hyparquet Writer

![hyparquet writer parakeet](hyparquet-writer.jpg)

[![npm](https://img.shields.io/npm/v/hyparquet-writer)](https://www.npmjs.com/package/hyparquet-writer)
[![minzipped](https://img.shields.io/bundlephobia/minzip/hyparquet-writer)](https://www.npmjs.com/package/hyparquet-writer)
[![workflow status](https://github.com/hyparam/hyparquet-writer/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/hyparquet-writer/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-97-darkred)
[![dependencies](https://img.shields.io/badge/Dependencies-1-blueviolet)](https://www.npmjs.com/package/hyparquet-writer?activeTab=dependencies)

Hyparquet Writer is a JavaScript library for writing [Apache Parquet](https://parquet.apache.org) files. It is designed to be lightweight, fast and store data very efficiently. It is a companion to the [hyparquet](https://github.com/hyparam/hyparquet) library, which is a JavaScript library for reading parquet files.

## Usage

Call `parquetWrite` with argument `columnData`. Each column in `columnData` should contain:

- `name`: the column name
- `data`: an array of same-type values
- `type`: the parquet schema type (optional, type guessed from data if not provided)

Example:

```javascript
import { parquetWrite } from 'hyparquet-writer'

const arrayBuffer = parquetWrite({
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'STRING' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
})
```

## Options

Options can be passed to `parquetWrite` to change parquet file properties:

 - `compression`: use snappy compression (default true)
 - `statistics`: write column statistics (default true)
 - `rowGroupSize`: number of rows in each row group (default 100000)
 - `kvMetadata`: extra key-value metadata

```javascript
import { parquetWrite } from 'hyparquet-writer'

const arrayBuffer = parquetWrite({
  columnData: [
    { name: 'name', data: ['Alice', 'Bob', 'Charlie'], type: 'STRING' },
    { name: 'age', data: [25, 30, 35], type: 'INT32' },
  ],
  compression: false,
  statistics: false,
  rowGroupSize: 1000,
  kvMetadata: {
    'key1': 'value1',
    'key2': 'value2',
  },
})
```

## References

 - https://github.com/hyparam/hyparquet
 - https://github.com/hyparam/hyparquet-compressors
 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
