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
      metadata_length: 529,
    }
    writeMetadata(writer, withKvMetadata)

    // write footer PAR1
    writer.appendUint32(0x31524150)

    const file = writer.getBuffer()
    const outputMetadata = parquetMetadata(file)

    expect(outputMetadata).toEqual(withKvMetadata)
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
