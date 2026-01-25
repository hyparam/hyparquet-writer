// @ts-nocheck - ALP encoding not yet in hyparquet types
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

// Note: Full round-trip tests require hyparquet with ALP support (version > 1.23.2)
// The encoder-level tests in alp.test.js verify the round-trip using the local hyparquet

describe('parquetWrite with ALP encoding', () => {
  it('should write float data with ALP encoding', () => {
    const data = [1.23, 4.56, 7.89, 0.12, 19.99, 5.49]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'price', data, encoding: 'ALP' }],
    })
    // File should be created without errors
    expect(file.byteLength).toBeGreaterThan(0)
  })

  it('should write double data with ALP encoding', () => {
    const data = [1.23, 4.56, 7.89, 0.12, 19.99, 5.49]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'price', data, type: 'DOUBLE', encoding: 'ALP' }],
    })
    expect(file.byteLength).toBeGreaterThan(0)
  })

  it('should handle exceptions in ALP encoding', () => {
    const data = [1.5, NaN, 2.5, Infinity, -Infinity]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'value', data, type: 'DOUBLE', encoding: 'ALP' }],
    })
    expect(file.byteLength).toBeGreaterThan(0)
  })

  it('should write large dataset with ALP encoding', () => {
    // More than 1024 values to test multiple vectors
    const data = Array.from({ length: 2000 }, (_, i) => i * 0.01)
    const file = parquetWriteBuffer({
      columnData: [{ name: 'value', data, type: 'DOUBLE', encoding: 'ALP' }],
    })
    expect(file.byteLength).toBeGreaterThan(0)
  })

  it('should write nullable values with ALP encoding', () => {
    const data = [1.5, null, 2.5, null, 3.5]
    const file = parquetWriteBuffer({
      columnData: [{ name: 'value', data, type: 'DOUBLE', encoding: 'ALP' }],
    })
    expect(file.byteLength).toBeGreaterThan(0)
  })

  it('should produce smaller file than PLAIN for decimal-like data', () => {
    // Monetary data should compress well with ALP
    const data = Array.from({ length: 1000 }, (_, i) => Math.round(i * 0.01 * 100) / 100)

    const alpFile = parquetWriteBuffer({
      columnData: [{ name: 'price', data, type: 'DOUBLE', encoding: 'ALP' }],
      codec: 'UNCOMPRESSED',
    })

    const plainFile = parquetWriteBuffer({
      columnData: [{ name: 'price', data, type: 'DOUBLE', encoding: 'PLAIN' }],
      codec: 'UNCOMPRESSED',
    })

    // ALP should be smaller for decimal-like data (uncompressed)
    expect(alpFile.byteLength).toBeLessThan(plainFile.byteLength)
  })

  it('should throw error for non-floating point types', () => {
    expect(() => {
      parquetWriteBuffer({
        columnData: [{ name: 'int', data: [1, 2, 3], type: 'INT32', encoding: 'ALP' }],
      })
    }).toThrow('ALP encoding only supported for FLOAT and DOUBLE types')
  })
})
