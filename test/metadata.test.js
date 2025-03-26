import { parquetMetadata } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { Writer } from '../src/writer.js'
import { writeMetadata } from '../src/metadata.js'

/**
 * @import {FileMetaData} from 'hyparquet'
 * @type {FileMetaData}
 */
export const exampleMetadata = {
  version: 2,
  created_by: 'hyparquet',
  schema: [
    { name: 'root', num_children: 6 },
    { name: 'bool', type: 'BOOLEAN', repetition_type: 'REQUIRED' },
    { name: 'int', type: 'INT32', repetition_type: 'REQUIRED' },
    { name: 'bigint', type: 'INT64', repetition_type: 'REQUIRED' },
    { name: 'double', type: 'DOUBLE', repetition_type: 'REQUIRED' },
    { name: 'string', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED' },
    { name: 'nullable', type: 'BOOLEAN', repetition_type: 'OPTIONAL' },
  ],
  num_rows: 4n,
  row_groups: [{
    columns: [
      {
        file_path: 'bool',
        file_offset: 4n,
        meta_data: {
          type: 'BOOLEAN',
          encodings: ['PLAIN'],
          path_in_schema: ['bool'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 23n,
          total_compressed_size: 23n,
          data_page_offset: 4n,
        },
      },
      {
        file_path: 'int',
        file_offset: 27n,
        meta_data: {
          type: 'INT32',
          encodings: ['PLAIN'],
          path_in_schema: ['int'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 38n,
          total_compressed_size: 38n,
          data_page_offset: 27n,
        },
      },
      {
        file_path: 'bigint',
        file_offset: 65n,
        meta_data: {
          type: 'INT64',
          encodings: ['PLAIN'],
          path_in_schema: ['bigint'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 54n,
          total_compressed_size: 54n,
          data_page_offset: 65n,
        },
      },
      {
        file_path: 'double',
        file_offset: 119n,
        meta_data: {
          type: 'DOUBLE',
          encodings: ['PLAIN'],
          path_in_schema: ['double'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 54n,
          total_compressed_size: 54n,
          data_page_offset: 119n,
        },
      },
      {
        file_path: 'string',
        file_offset: 173n,
        meta_data: {
          type: 'BYTE_ARRAY',
          encodings: ['PLAIN'],
          path_in_schema: ['string'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 42n,
          total_compressed_size: 42n,
          data_page_offset: 173n,
        },
      },
      {
        file_path: 'nullable',
        file_offset: 215n,
        meta_data: {
          type: 'BOOLEAN',
          encodings: ['PLAIN'],
          path_in_schema: ['nullable'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 25n,
          total_compressed_size: 25n,
          data_page_offset: 215n,
        },
      },
    ],
    total_byte_size: 236n,
    num_rows: 4n,
  }],
  metadata_length: 336,
}

describe('writeMetadata', () => {
  it('writes metadata and parses in hyparquet', () => {
    const writer = new Writer()

    // Write header PAR1
    writer.appendUint32(0x31524150)

    // Write metadata
    /** @type {FileMetaData} */
    writeMetadata(writer, exampleMetadata)

    // Write footer PAR1
    writer.appendUint32(0x31524150)

    const file = writer.getBuffer()
    const output = parquetMetadata(file)

    /** @type {FileMetaData} */
    expect(output).toEqual(exampleMetadata)
  })

})
