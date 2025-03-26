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
    { name: 'root', num_children: 4 },
    { name: 'bool', type: 'BOOLEAN' },
    { name: 'int', type: 'INT32' },
    { name: 'bigint', type: 'INT64' },
    { name: 'double', type: 'DOUBLE' },
  ],
  num_rows: 4n,
  row_groups: [{
    columns: [
      {
        file_path: 'bool',
        file_offset: 32n,
        meta_data: {
          type: 'BOOLEAN',
          encodings: ['PLAIN'],
          path_in_schema: ['bool'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 28n,
          total_compressed_size: 28n,
          data_page_offset: 4n,
        },
      },
      {
        file_path: 'int',
        file_offset: 75n,
        meta_data: {
          type: 'INT32',
          encodings: ['PLAIN'],
          path_in_schema: ['int'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 43n,
          total_compressed_size: 43n,
          data_page_offset: 32n,
        },
      },
      {
        file_path: 'bigint',
        file_offset: 134n,
        meta_data: {
          type: 'INT64',
          encodings: ['PLAIN'],
          path_in_schema: ['bigint'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 59n,
          total_compressed_size: 59n,
          data_page_offset: 75n,
        },
      },
      {
        file_path: 'double',
        file_offset: 193n,
        meta_data: {
          type: 'DOUBLE',
          encodings: ['PLAIN'],
          path_in_schema: ['double'],
          codec: 'UNCOMPRESSED',
          num_values: 4n,
          total_uncompressed_size: 59n,
          total_compressed_size: 59n,
          data_page_offset: 134n,
        },
      },
    ],
    total_byte_size: 189n,
    num_rows: 4n,
  }],
  metadata_length: 219,
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
