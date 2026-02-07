import { parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * Roundtrip helper: write with parquetWriteBuffer, read back with parquetReadObjects.
 *
 * @param {import('../src/types.js').ColumnSource[]} columnData
 * @returns {Promise<Record<string, any>[]>}
 */
async function roundTrip(columnData) {
  const file = parquetWriteBuffer({ columnData })
  return await parquetReadObjects({ file })
}

describe('variant writing', () => {
  it('encodes null values', async () => {
    const data = [null, null, null]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual([{ v: null }, { v: null }, { v: null }])
  })

  it('encodes booleans', async () => {
    const data = [true, false, null]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual([{ v: true }, { v: false }, { v: null }])
  })

  it('encodes integers', async () => {
    const data = [0, 127, -128, 32767, -32768, 2147483647, -2147483648]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes bigints', async () => {
    const data = [0n, 1n, -1n, 9007199254740993n]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('throws on bigint values outside int64 range', () => {
    const outOfRange = [2n ** 63n, -(2n ** 63n) - 1n]
    for (const value of outOfRange) {
      expect(() => parquetWriteBuffer({
        columnData: [{ name: 'v', data: [value], type: 'VARIANT' }],
      })).toThrow(/int64 range/)
    }
  })

  it('encodes doubles', async () => {
    const data = [3.14, -0.001, 1e100, Number.MIN_VALUE]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes short strings', async () => {
    const data = ['hello', '', 'a', 'short string test']
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes long strings', async () => {
    const longStr = 'x'.repeat(100)
    const data = [longStr]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual([{ v: longStr }])
  })

  it('encodes Dates as timestamps', async () => {
    const d1 = new Date('2024-01-15T10:30:00.000Z')
    const d2 = new Date('2000-06-01T00:00:00.000Z')
    const data = [d1, d2]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    // Reader returns Date objects from timestamp_micros_ntz
    expect(result[0].v.getTime()).toBe(d1.getTime())
    expect(result[1].v.getTime()).toBe(d2.getTime())
  })

  it('encodes Uint8Array as binary', async () => {
    const data = [new Uint8Array([1, 2, 3]), new Uint8Array([0xff])]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result[0].v).toEqual(new Uint8Array([1, 2, 3]))
    expect(result[1].v).toEqual(new Uint8Array([0xff]))
  })

  it('encodes objects', async () => {
    const data = [
      { a: 1, b: 'hello' },
      { a: 2, b: 'world' },
    ]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes arrays', async () => {
    const data = [
      [1, 2, 3],
      ['a', 'b'],
    ]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes nested objects and arrays', async () => {
    const data = [
      { name: 'test', tags: [1, 2, 3], nested: { x: true } },
      { name: 'other', tags: [], nested: { x: false } },
    ]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes mixed types in one column', async () => {
    const data = [42, 'hello', true, null, [1, 2], { key: 'val' }]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual([
      { v: 42 }, { v: 'hello' }, { v: true }, { v: null },
      { v: [1, 2] }, { v: { key: 'val' } },
    ])
  })

  it('encodes empty objects and arrays', async () => {
    const data = [{}, []]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('encodes variant alongside other columns', async () => {
    const result = await roundTrip([
      { name: 'id', data: [1, 2], type: 'INT32' },
      { name: 'v', data: [{ a: 1 }, { b: 2 }], type: 'VARIANT' },
    ])
    expect(result).toEqual([
      { id: 1, v: { a: 1 } },
      { id: 2, v: { b: 2 } },
    ])
  })

  it('handles variant-null in value (null inside non-null group)', async () => {
    const data = [{ key: null }, null]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual([{ v: { key: null } }, { v: null }])
  })

  it('treats undefined as missing at top level', async () => {
    const data = [undefined, { key: 1 }]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT' }])
    expect(result).toEqual([{ v: undefined }, { v: { key: 1 } }])
  })

  it('allows top-level null in required variant columns', async () => {
    const data = [null]
    const result = await roundTrip([{ name: 'v', data, type: 'VARIANT', nullable: false }])
    expect(result).toEqual([{ v: null }])
  })
})
