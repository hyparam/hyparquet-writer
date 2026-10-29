import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

describe('parquetWrite with ALP encoding', () => {
  it('should round-trip float data', async () => {
    const data = [1.23, 4.56, 7.89, 0.12, 19.99, 5.49]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'price', data, type: 'FLOAT', encoding: 'ALP' }],
    })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toContain('ALP')
    const rows = await parquetReadObjects({ file })
    expect(rows.map(r => r.price)).toEqual(data.map(Math.fround))
  })

  it('should round-trip double data with exceptions and nulls', async () => {
    const data = [1.5, NaN, null, 2.5, Infinity, -Infinity, -0, null, 1 / 3]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'value', data, type: 'DOUBLE', encoding: 'ALP' }],
    })
    const rows = await parquetReadObjects({ file })
    expect(rows.map(r => r.value)).toEqual(data)
  })

  it('should round-trip multiple vectors', async () => {
    const data = Array.from({ length: 5000 }, (_, i) => i * 0.01)
    const file = parquetWriteBuffer({
      columnData: [{ name: 'value', data, type: 'DOUBLE', encoding: 'ALP' }],
    })
    const rows = await parquetReadObjects({ file })
    expect(rows.map(r => r.value)).toEqual(data)
  })

  it('should produce smaller file than PLAIN for decimal-like data', () => {
    const data = Array.from({ length: 1000 }, (_, i) => Math.round(i * 0.01 * 100) / 100)
    const alpFile = parquetWriteBuffer({
      columnData: [{ name: 'price', data, type: 'DOUBLE', encoding: 'ALP' }],
      codec: 'UNCOMPRESSED',
    })
    const plainFile = parquetWriteBuffer({
      columnData: [{ name: 'price', data, type: 'DOUBLE', encoding: 'PLAIN' }],
      codec: 'UNCOMPRESSED',
    })
    expect(alpFile.byteLength).toBeLessThan(plainFile.byteLength / 2)
  })

  it('should throw error for non-floating point types', () => {
    expect(() => {
      parquetWriteBuffer({
        columnData: [{ name: 'int', data: [1, 2, 3], type: 'INT32', encoding: 'ALP' }],
      })
    }).toThrow('ALP encoding only supported for FLOAT and DOUBLE types')
  })
})
