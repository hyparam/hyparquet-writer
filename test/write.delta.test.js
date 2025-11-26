import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

describe('DELTA_BINARY_PACKED encoding', () => {
  it('writes DELTA_BINARY_PACKED encoding for INT32', async () => {
    const data = [1, 2, 3, 100, 200, 300]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'int', data, encoding: 'DELTA_BINARY_PACKED' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['DELTA_BINARY_PACKED'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(int => ({ int })))
  })

  it('writes DELTA_BINARY_PACKED encoding for INT64', async () => {
    const data = [1n, 2n, 3n, 100n, 200n, 300n]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'bigint', data, encoding: 'DELTA_BINARY_PACKED' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['DELTA_BINARY_PACKED'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(bigint => ({ bigint })))
  })
})

describe('DELTA_LENGTH_BYTE_ARRAY encoding', () => {
  it('writes DELTA_LENGTH_BYTE_ARRAY encoding for strings', async () => {
    const data = ['hello', 'world', 'foo', 'bar', 'baz', 'qux']
    const file = parquetWriteBuffer({
      columnData: [{ name: 'string', data, encoding: 'DELTA_LENGTH_BYTE_ARRAY' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['DELTA_LENGTH_BYTE_ARRAY'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(string => ({ string })))
  })

  it('writes DELTA_LENGTH_BYTE_ARRAY encoding for byte arrays', async () => {
    const data = [
      Uint8Array.of(1, 2, 3),
      Uint8Array.of(4, 5, 6, 7),
      Uint8Array.of(8, 9),
      Uint8Array.of(10, 11, 12, 13, 14),
    ]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'bytes', data, encoding: 'DELTA_LENGTH_BYTE_ARRAY' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['DELTA_LENGTH_BYTE_ARRAY'])
    const result = await parquetReadObjects({ file, utf8: false })
    expect(result).toEqual(data.map(bytes => ({ bytes })))
  })
})

describe('DELTA_BYTE_ARRAY encoding', () => {
  it('writes DELTA_BYTE_ARRAY encoding for strings with common prefixes', async () => {
    const data = ['apple', 'application', 'apply', 'banana', 'band', 'bandana']
    const file = parquetWriteBuffer({
      columnData: [{ name: 'string', data, encoding: 'DELTA_BYTE_ARRAY' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['DELTA_BYTE_ARRAY'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(string => ({ string })))
  })

  it('writes DELTA_BYTE_ARRAY encoding for byte arrays', async () => {
    const data = [
      Uint8Array.of(1, 2, 3, 4),
      Uint8Array.of(1, 2, 5, 6),
      Uint8Array.of(1, 2, 7, 8),
      Uint8Array.of(10, 11, 12, 13),
    ]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'bytes', data, encoding: 'DELTA_BYTE_ARRAY' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['DELTA_BYTE_ARRAY'])
    const result = await parquetReadObjects({ file, utf8: false })
    expect(result).toEqual(data.map(bytes => ({ bytes })))
  })
})
