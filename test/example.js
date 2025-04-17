/** @type {ColumnData[]} */
export const exampleData = [
  { name: 'bool', data: [true, false, true, false] },
  { name: 'int', data: [0, 127, 0x7fff, 0x7fffffff] },
  { name: 'bigint', data: [0n, 127n, 0x7fffn, 0x7fffffffffffffffn] },
  { name: 'float', data: [0, 0.0001, 123.456, 1e100], type: 'FLOAT', repetition_type: 'REQUIRED' },
  { name: 'double', data: [0, 0.0001, 123.456, 1e100] },
  { name: 'string', data: ['a', 'b', 'c', 'd'] },
  { name: 'nullable', data: [true, false, null, null] },
]

/**
 * @import {FileMetaData, LogicalType} from 'hyparquet'
 * @import {ColumnData, ThriftObject} from '../src/types.js'
 * @type {FileMetaData}
 */
export const exampleMetadata = {
  version: 2,
  created_by: 'hyparquet',
  schema: [
    { name: 'root', num_children: 7 },
    { name: 'bool', type: 'BOOLEAN', repetition_type: 'REQUIRED' },
    { name: 'int', type: 'INT32', repetition_type: 'REQUIRED' },
    { name: 'bigint', type: 'INT64', repetition_type: 'REQUIRED' },
    { name: 'float', type: 'FLOAT', repetition_type: 'REQUIRED' },
    { name: 'double', type: 'DOUBLE', repetition_type: 'REQUIRED' },
    { name: 'string', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED', converted_type: 'UTF8' },
    { name: 'nullable', type: 'BOOLEAN', repetition_type: 'OPTIONAL' },
  ],
  num_rows: 4n,
  row_groups: [{
    columns: [
      {
        file_offset: 4n,
        meta_data: {
          type: 'BOOLEAN',
          encodings: ['PLAIN'],
          path_in_schema: ['bool'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 24n,
          total_compressed_size: 24n,
          data_page_offset: 4n,
          statistics: {
            null_count: 0n,
            min_value: false,
            max_value: true,
          },
        },
      },
      {
        file_offset: 28n,
        meta_data: {
          type: 'INT32',
          encodings: ['PLAIN'],
          path_in_schema: ['int'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 39n,
          total_compressed_size: 39n,
          data_page_offset: 28n,
          statistics: {
            null_count: 0n,
            min_value: 0,
            max_value: 0x7fffffff,
          },
        },
      },
      {
        file_offset: 67n,
        meta_data: {
          type: 'INT64',
          encodings: ['PLAIN'],
          path_in_schema: ['bigint'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 43n,
          total_compressed_size: 43n,
          data_page_offset: 67n,
          statistics: {
            null_count: 0n,
            min_value: 0n,
            max_value: 0x7fffffffffffffffn,
          },
        },
      },
      {
        file_offset: 110n,
        meta_data: {
          type: 'FLOAT',
          encodings: ['PLAIN'],
          path_in_schema: ['float'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 39n,
          total_compressed_size: 39n,
          data_page_offset: 110n,
          statistics: {
            null_count: 0n,
            min_value: 0,
            max_value: Infinity,
          },
        },
      },
      {
        file_offset: 149n,
        meta_data: {
          type: 'DOUBLE',
          encodings: ['PLAIN'],
          path_in_schema: ['double'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 51n,
          total_compressed_size: 51n,
          data_page_offset: 149n,
          statistics: {
            null_count: 0n,
            min_value: 0,
            max_value: 1e100,
          },
        },
      },
      {
        file_offset: 200n,
        meta_data: {
          type: 'BYTE_ARRAY',
          encodings: ['PLAIN'],
          path_in_schema: ['string'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 42n,
          total_compressed_size: 42n,
          data_page_offset: 200n,
          statistics: {
            null_count: 0n,
            min_value: 'a',
            max_value: 'd',
          },
        },
      },
      {
        file_offset: 242n,
        meta_data: {
          type: 'BOOLEAN',
          encodings: ['PLAIN'],
          path_in_schema: ['nullable'],
          codec: 'SNAPPY',
          num_values: 4n,
          total_uncompressed_size: 26n,
          total_compressed_size: 26n,
          data_page_offset: 242n,
          statistics: {
            null_count: 2n,
            min_value: false,
            max_value: true,
          },
        },
      },
    ],
    total_byte_size: 264n,
    num_rows: 4n,
  }],
  metadata_length: 445,
}
