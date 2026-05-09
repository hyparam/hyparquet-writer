import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'
import { autoDetectShredding } from '../src/variant.js'

/**
 * @import {ColumnChunk, FileMetaData} from 'hyparquet'
 * @import {ColumnSource} from '../src/types.js'
 */

/**
 * Roundtrip helper: write with parquetWriteBuffer, read back with parquetReadObjects.
 *
 * @param {ColumnSource[]} columnData
 * @returns {Promise<Record<string, any>[]>}
 */
async function roundTrip(columnData) {
  const file = parquetWriteBuffer({ columnData })
  return await parquetReadObjects({ file })
}

/**
 * @param {FileMetaData} metadata
 * @param {string[]} path
 * @returns {ColumnChunk}
 */
function physicalColumn(metadata, path) {
  const column = metadata.row_groups[0].columns.find(column =>
    column.meta_data?.path_in_schema.join('.') === path.join('.')
  )
  if (!column) throw new Error(`column not found: ${path.join('.')}`)
  return column
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

  it('rejects undefined rows in required variant columns', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'v', data: [undefined], type: 'VARIANT', nullable: false }],
    })).toThrow(/variant.*required|required.*variant/i)
  })
})

describe('variant shredding', () => {
  it('shreds string fields', async () => {
    const data = [
      { event_type: 'login' },
      { event_type: 'logout' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('shreds int32 fields', async () => {
    const data = [
      { count: 42 },
      { count: 100 },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { count: 'INT32' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('shreds timestamp fields into the typed timestamp leaf', async () => {
    const data = [
      { d: new Date('2024-01-15T10:30:00.000Z') },
      { d: new Date('2000-06-01T00:00:00.000Z') },
    ]
    const file = parquetWriteBuffer({ columnData: [{
      name: 'v',
      data,
      type: 'VARIANT',
      shredding: { d: 'TIMESTAMP' },
    }] })
    const metadata = parquetMetadata(file)
    const typedLeaf = physicalColumn(metadata, ['v', 'typed_value', 'd', 'typed_value'])
    const fallbackLeaf = physicalColumn(metadata, ['v', 'typed_value', 'd', 'value'])
    expect(typedLeaf.meta_data?.statistics?.null_count).toBe(0n)
    expect(fallbackLeaf.meta_data?.statistics?.null_count).toBe(BigInt(data.length))

    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('falls back to binary value for int32 fields outside int32 range', async () => {
    const data = [
      { count: 2147483647 },
      { count: 2147483648 },
      { count: -2147483649 },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { count: 'INT32' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('throws on int64 shredded fields outside int64 range', () => {
    const outOfRange = [
      { count: 2n ** 63n },
      { count: -(2n ** 63n) - 1n },
    ]
    for (const value of outOfRange) {
      expect(() => parquetWriteBuffer({
        columnData: [{
          name: 'v',
          data: [value],
          type: 'VARIANT',
          shredding: { count: 'INT64' },
        }],
      })).toThrow(/int64 range/)
    }
  })

  it('shreds multiple fields', async () => {
    const data = [
      { event_type: 'login', count: 1 },
      { event_type: 'signup', count: 5 },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING', count: 'INT32' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('shreds partially (extra unshredded fields)', async () => {
    const data = [
      { event_type: 'login', email: 'user@example.com' },
      { event_type: 'signup', email: 'new@example.com' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('preserves absent shredded fields', async () => {
    const data = [
      { event_type: 'login', count: 1 },
      { event_type: 'click' },
      { event_type: 'purchase', count: null },
    ]
    const file = parquetWriteBuffer({ columnData: [{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING', count: 'INT32' },
    }] })
    const metadata = parquetMetadata(file)
    const countGroup = metadata.schema.find(element => element.name === 'count' && element.num_children === 2)
    expect(countGroup?.repetition_type).toBe('OPTIONAL')

    const result = await parquetReadObjects({ file })
    expect(result[0].v).toEqual({ event_type: 'login', count: 1 })
    expect(result[1].v).toEqual({ event_type: 'click' })
    expect(result[1].v).not.toHaveProperty('count')
    expect(result[2].v).toEqual({ event_type: 'purchase', count: null })
  })

  it('handles null values in variant column', async () => {
    const data = [
      { event_type: 'login' },
      null,
      { event_type: 'logout' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING' },
    }])
    expect(result[0].v).toEqual({ event_type: 'login' })
    expect(result[1].v).toEqual(null)
    expect(result[2].v).toEqual({ event_type: 'logout' })
  })

  it('handles non-object values alongside objects', async () => {
    const data = [
      { event_type: 'login' },
      'not an object',
      42,
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING' },
    }])
    expect(result[0].v).toEqual({ event_type: 'login' })
    expect(result[1].v).toEqual('not an object')
    expect(result[2].v).toEqual(42)
  })

  it('handles empty objects', async () => {
    const data = [
      { event_type: 'login' },
      {},
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING' },
    }])
    expect(result[0].v).toEqual({ event_type: 'login' })
    expect(result[1].v).toEqual({})
  })

  it('auto-detects shredding config', async () => {
    const data = [
      { event_type: 'login', count: 1.5 },
      { event_type: 'signup', count: 2.0 },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: true,
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('does not auto-shred reserved variant wrapper fields', async () => {
    const data = [
      { value: 'x' },
      { typed_value: 'y' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: true,
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('does not explicitly shred reserved variant wrapper fields', async () => {
    const data = [
      { value: 'x', typed_value: 'y', name: 'Alice' },
      { value: 'z', typed_value: 'w', name: 'Bob' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { value: 'STRING', typed_value: 'STRING', name: 'STRING' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('auto-detects timestamp shredding config', async () => {
    const data = [
      { d: new Date('2024-01-15T10:30:00.000Z') },
      { d: new Date('2000-06-01T00:00:00.000Z') },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: true,
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('shreds boolean fields', async () => {
    const data = [
      { active: true, name: 'a' },
      { active: false, name: 'b' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { active: 'BOOLEAN', name: 'STRING' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('shreds double fields', async () => {
    const data = [
      { score: 3.14 },
      { score: 2.718 },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { score: 'DOUBLE' },
    }])
    expect(result).toEqual(data.map(v => ({ v })))
  })

  it('handles type mismatch (string where int expected)', async () => {
    const data = [
      { count: 42 },
      { count: 'not a number' },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { count: 'INT32' },
    }])
    expect(result[0].v).toEqual({ count: 42 })
    // Type mismatch falls back to binary value
    expect(result[1].v).toEqual({ count: 'not a number' })
  })

  it('handles null field values in objects', async () => {
    const data = [
      { event_type: 'login' },
      { event_type: null },
    ]
    const result = await roundTrip([{
      name: 'v', data, type: 'VARIANT',
      shredding: { event_type: 'STRING' },
    }])
    expect(result[0].v).toEqual({ event_type: 'login' })
    expect(result[1].v).toEqual({ event_type: null })
  })

  it('autoDetectShredding detects consistent types', () => {
    const values = [
      { name: 'Alice', age: 30.0 },
      { name: 'Bob', age: 25.0 },
      null,
      { name: 'Charlie' },
    ]
    const config = autoDetectShredding(values)
    expect(config).toEqual({ name: 'STRING', age: 'DOUBLE' })
  })

  it('autoDetectShredding excludes mixed-type fields', () => {
    const values = [
      { name: 'Alice', score: 100 },
      { name: 'Bob', score: 'high' },
    ]
    const config = autoDetectShredding(values)
    expect(config).toEqual({ name: 'STRING' })
  })

  it('autoDetectShredding excludes reserved variant wrapper fields', () => {
    const values = [
      { value: 'x', typed_value: 'y', name: 'Alice' },
      { value: 'z', typed_value: 'w', name: 'Bob' },
    ]
    const config = autoDetectShredding(values)
    expect(config).toEqual({ name: 'STRING' })
  })

  it('autoDetectShredding returns undefined for non-objects', () => {
    expect(autoDetectShredding([1, 2, 3])).toBeUndefined()
    expect(autoDetectShredding([null, null])).toBeUndefined()
  })
})
