import { parquetMetadata } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { logicalType, writeMetadata } from '../src/metadata.js'
import { exampleMetadata } from './example.js'

/**
 * @import {FileMetaData, LogicalType} from 'hyparquet'
 * @import {ThriftObject} from '../src/types.js'
 */

describe('writeMetadata', () => {
  it('writes metadata and parses in hyparquet', () => {
    const writer = new ByteWriter()

    // write header PAR1
    writer.appendUint32(0x31524150)

    // write metadata
    /** @type {FileMetaData} */
    const withKvMetadata = {
      ...exampleMetadata,
      key_value_metadata: [
        { key: 'key1', value: 'value1' },
        { key: 'key2', value: 'value2' },
      ],
      metadata_length: 540,
    }
    writeMetadata(writer, withKvMetadata)

    // write footer PAR1
    writer.appendUint32(0x31524150)

    const file = writer.getBuffer()
    const outputMetadata = parquetMetadata(file)

    expect(outputMetadata).toEqual(withKvMetadata)
  })

  it('writes extended column metadata fields', () => {
    const writer = new ByteWriter()
    writer.appendUint32(0x31524150)

    /** @type {FileMetaData} */
    const extendedMetadata = {
      version: 2,
      created_by: 'hyparquet',
      schema: [
        { name: 'root', num_children: 1 },
        {
          name: 'geo',
          type: 'BYTE_ARRAY',
          repetition_type: 'REQUIRED',
          logical_type: { type: 'GEOGRAPHY', crs: 'EPSG:4326', algorithm: 'KARNEY' },
        },
      ],
      num_rows: 1n,
      row_groups: [{
        columns: [{
          file_path: 'part-0.parquet',
          file_offset: 4n,
          meta_data: {
            type: 'BYTE_ARRAY',
            encodings: ['PLAIN', 'RLE'],
            path_in_schema: ['geo'],
            codec: 'SNAPPY',
            num_values: 1n,
            total_uncompressed_size: 10n,
            total_compressed_size: 8n,
            key_value_metadata: [{ key: 'chunk', value: 'value' }],
            data_page_offset: 4n,
            index_page_offset: 12n,
            dictionary_page_offset: 20n,
            statistics: {
              null_count: 0n,
              min_value: 'a',
              max_value: 'z',
            },
            encoding_stats: [{ page_type: 'DATA_PAGE', encoding: 'PLAIN', count: 1 }],
            bloom_filter_offset: 30n,
            bloom_filter_length: 4,
            size_statistics: {
              unencoded_byte_array_data_bytes: 5n,
              repetition_level_histogram: [1n, 0n],
              definition_level_histogram: [2n, 0n],
            },
            geospatial_statistics: {
              bbox: {
                xmin: 0,
                xmax: 10,
                ymin: -5,
                ymax: 5,
                zmin: 1,
                zmax: 2,
                mmin: 3,
                mmax: 4,
              },
              geospatial_types: [0, 1],
            },
          },
          offset_index_offset: 40n,
          offset_index_length: 16,
          column_index_offset: 60n,
          column_index_length: 24,
          encrypted_column_metadata: new Uint8Array([7, 8, 9]),
        }],
        total_byte_size: 64n,
        num_rows: 1n,
        sorting_columns: [{
          column_idx: 0,
          descending: true,
          nulls_first: false,
        }],
        file_offset: 4n,
        total_compressed_size: 8n,
      }],
      key_value_metadata: [{ key: 'meta', value: 'data' }],
      metadata_length: 227,
    }

    writeMetadata(writer, extendedMetadata)
    writer.appendUint32(0x31524150)

    const outputMetadata = parquetMetadata(writer.getBuffer())
    expect(outputMetadata).toEqual(extendedMetadata)
  })
})

describe('logicalType', () => {
  it('returns undefined when given undefined', () => {
    expect(logicalType(undefined)).toBeUndefined()
  })

  it('returns correct object for known types', () => {
    /** @type {{ input: LogicalType, expected: ThriftObject }[]} */
    const testCases = [
      { input: { type: 'STRING' }, expected: { field_1: {} } },
      { input: { type: 'MAP' }, expected: { field_2: {} } },
      { input: { type: 'LIST' }, expected: { field_3: {} } },
      { input: { type: 'ENUM' }, expected: { field_4: {} } },
      {
        input: { type: 'DECIMAL', scale: 2, precision: 5 },
        expected: { field_5: { field_1: 2, field_2: 5 } },
      },
      { input: { type: 'DATE' }, expected: { field_6: {} } },
      {
        input: { type: 'TIME', isAdjustedToUTC: true, unit: 'MILLIS' },
        expected: { field_7: { field_1: true, field_2: { field_1: {} } } },
      },
      {
        input: { type: 'TIMESTAMP', isAdjustedToUTC: false, unit: 'MICROS' },
        expected: { field_8: { field_1: false, field_2: { field_2: {} } } },
      },
      {
        input: { type: 'TIMESTAMP', isAdjustedToUTC: false, unit: 'NANOS' },
        expected: { field_8: { field_1: false, field_2: { field_3: {} } } },
      },
      {
        input: { type: 'INTEGER', bitWidth: 32, isSigned: true },
        expected: { field_10: { field_1: 32, field_2: true } },
      },
      { input: { type: 'NULL' }, expected: { field_11: {} } },
      { input: { type: 'JSON' }, expected: { field_12: {} } },
      { input: { type: 'BSON' }, expected: { field_13: {} } },
      { input: { type: 'UUID' }, expected: { field_14: {} } },
      { input: { type: 'FLOAT16' }, expected: { field_15: {} } },
      { input: { type: 'VARIANT' }, expected: { field_16: {} } },
      { input: { type: 'GEOMETRY' }, expected: { field_17: {} } },
      { input: { type: 'GEOGRAPHY' }, expected: { field_18: {} } },
    ]

    testCases.forEach(({ input, expected }) => {
      expect(logicalType(input)).toEqual(expected)
    })
  })
})
