import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWrite } from '../src/index.js'
import { exampleMetadata } from './metadata.test.js'

/**
 * Utility to encode a parquet file and then read it back into a JS object.
 *
 * @param {Record<string, any[]>} columnData
 * @returns {Promise<Record<string, any>>}
 */
async function roundTripDeserialize(columnData) {
  const file = parquetWrite(columnData)
  return await parquetReadObjects({ file })
}

const data = {
  bool: [true, false, true, false], // BOOLEAN
  int: [0, 127, 0x7fff, 0x7fffffff], // INT32
  bigint: [0n, 127n, 0x7fffn, 0x7fffffffffffffffn], // INT64
  double: [0, 0.0001, 123.456, 1e100], // DOUBLE
  string: ['a', 'b', 'c', 'd'], // BYTE_ARRAY
}

describe('parquetWrite', () => {
  it('writes expected metadata', () => {
    const file = parquetWrite(data)
    const metadata = parquetMetadata(file)
    expect(metadata).toEqual(exampleMetadata)
  })

  it('serializes basic types correctly', async () => {
    const result = await roundTripDeserialize(data)
    expect(result).toEqual([
      { bool: true, int: 0, bigint: 0n, double: 0, string: 'a' },
      { bool: false, int: 127, bigint: 127n, double: 0.0001, string: 'b' },
      { bool: true, int: 0x7fff, bigint: 0x7fffn, double: 123.456, string: 'c' },
      { bool: false, int: 0x7fffffff, bigint: 0x7fffffffffffffffn, double: 1e100, string: 'd' },
    ])
  })
})
