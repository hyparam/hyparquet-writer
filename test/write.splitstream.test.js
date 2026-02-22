import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

describe('BYTE_STREAM_SPLIT encoding', () => {
  it('writes BYTE_STREAM_SPLIT encoding for FLOAT', async () => {
    const data = [1.5, 2.25, 3.125, -4.5, 0.0, 100.75]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'float', data, type: 'FLOAT', encoding: 'BYTE_STREAM_SPLIT' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.schema[1].type).toBe('FLOAT')
    const columnMetadata = metadata.row_groups[0].columns[0].meta_data
    expect(columnMetadata?.encodings).toEqual(['BYTE_STREAM_SPLIT'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(float => ({ float })))
  })

  it('writes BYTE_STREAM_SPLIT encoding for DOUBLE', async () => {
    const data = [1.5, 2.25, 3.125, -4.5, 0.0, 100.75, 1e100, -1e-100]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'double', data, type: 'DOUBLE', encoding: 'BYTE_STREAM_SPLIT' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.schema[1].type).toBe('DOUBLE')
    const columnMetadata = metadata.row_groups[0].columns[0].meta_data
    expect(columnMetadata?.encodings).toEqual(['BYTE_STREAM_SPLIT'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(double => ({ double })))
  })

  it('writes BYTE_STREAM_SPLIT encoding for INT32', async () => {
    const data = [1, 2, 3, -100, 0, 2147483647, -2147483648]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'int', data, encoding: 'BYTE_STREAM_SPLIT' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.schema[1].type).toBe('INT32')
    const columnMetadata = metadata.row_groups[0].columns[0].meta_data
    expect(columnMetadata?.encodings).toEqual(['BYTE_STREAM_SPLIT'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(int => ({ int })))
  })

  it('writes BYTE_STREAM_SPLIT encoding for INT64', async () => {
    const data = [1n, 2n, 3n, -100n, 0n, 9223372036854775807n, -9223372036854775808n]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'bigint', data, encoding: 'BYTE_STREAM_SPLIT' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.schema[1].type).toBe('INT64')
    const columnMetadata = metadata.row_groups[0].columns[0].meta_data
    expect(columnMetadata?.encodings).toEqual(['BYTE_STREAM_SPLIT'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(bigint => ({ bigint })))
  })

  it('writes BYTE_STREAM_SPLIT encoding with nulls', async () => {
    const data = [1.5, null, 3.125, null, 0.0, 100.75]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'float', data, encoding: 'BYTE_STREAM_SPLIT' }],
    })
    const metadata = parquetMetadata(file)
    const columnMetadata = metadata.row_groups[0].columns[0].meta_data
    expect(columnMetadata?.encodings).toEqual(['BYTE_STREAM_SPLIT'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(float => ({ float })))
  })

  it('writes BYTE_STREAM_SPLIT encoding with compression', async () => {
    const data = Array.from({ length: 1000 }, (_, i) => i * 0.1)
    const file = parquetWriteBuffer({
      columnData: [{ name: 'float', data, encoding: 'BYTE_STREAM_SPLIT' }],
    })
    const metadata = parquetMetadata(file)
    const columnMetadata = metadata.row_groups[0].columns[0].meta_data
    expect(columnMetadata?.encodings).toEqual(['BYTE_STREAM_SPLIT'])
    expect(columnMetadata?.codec).toBe('SNAPPY')
    const result = await parquetReadObjects({ file })
    expect(result.length).toBe(1000)
    result.forEach((row, i) => {
      expect(row.float).toBe(i * 0.1)
    })
  })
})
