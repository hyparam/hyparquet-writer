import { parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

describe('parquetWrite structs', () => {
  it('writes nested struct columns', async () => {
    const people = [
      { name: 'Ada', address: { city: 'London' } },
      null,
      { name: 'Ben', address: null },
      { name: 'Cara', address: { city: undefined } },
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'person', data: people }],
      schema: [
        { name: 'root', num_children: 1 },
        {
          name: 'person',
          repetition_type: 'OPTIONAL',
          num_children: 2,
        },
        {
          name: 'name',
          repetition_type: 'REQUIRED',
          type: 'BYTE_ARRAY',
          converted_type: 'UTF8',
        },
        {
          name: 'address',
          repetition_type: 'OPTIONAL',
          num_children: 1,
        },
        {
          name: 'city',
          repetition_type: 'OPTIONAL',
          type: 'BYTE_ARRAY',
          converted_type: 'UTF8',
        },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { person: { name: 'Ada', address: { city: 'London' } } },
      { person: undefined },
      { person: { name: 'Ben', address: undefined } },
      { person: { name: 'Cara', address: { city: null } } },
    ])
  })

  it('writes list of structs', async () => {
    const data = [
      [{ x: 1, y: 2 }, { x: 3, y: 4 }],
      [],
      [{ x: 5, y: null }],
      null,
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'points', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'points', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        { name: 'element', repetition_type: 'OPTIONAL', num_children: 2 },
        { name: 'x', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'y', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { points: [{ x: 1, y: 2 }, { x: 3, y: 4 }] },
      { points: [] },
      { points: [{ x: 5, y: null }] },
      { points: undefined },
    ])
  })

  it('writes struct containing list', async () => {
    const data = [
      { name: 'Alice', scores: [100, 95] },
      { name: 'Bob', scores: [] },
      { name: 'Carol', scores: null },
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'student', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'student', repetition_type: 'OPTIONAL', num_children: 2 },
        { name: 'name', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
        { name: 'scores', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        { name: 'element', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { student: { name: 'Alice', scores: [100, 95] } },
      { student: { name: 'Bob', scores: [] } },
      { student: { name: 'Carol', scores: undefined } },
    ])
  })

  it('writes map with struct values', async () => {
    const data = [
      { a: { count: 1 }, b: { count: 2 } },
      {},
      { c: { count: null } },
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'counts', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'counts', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
        { name: 'value', repetition_type: 'OPTIONAL', num_children: 1 },
        { name: 'count', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    // hyparquet reconstructs MAPs as objects with string keys
    expect(rows).toEqual([
      { counts: { a: { count: 1 }, b: { count: 2 } } },
      { counts: {} },
      { counts: { c: { count: null } } },
    ])
  })

  it('writes deeply nested struct (3 levels)', async () => {
    const data = [
      { a: { b: { c: 42 } } },
      { a: { b: null } },
      { a: null },
      null,
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'deep', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'deep', repetition_type: 'OPTIONAL', num_children: 1 },
        { name: 'a', repetition_type: 'OPTIONAL', num_children: 1 },
        { name: 'b', repetition_type: 'OPTIONAL', num_children: 1 },
        { name: 'c', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { deep: { a: { b: { c: 42 } } } },
      { deep: { a: { b: undefined } } },
      { deep: { a: undefined } },
      { deep: undefined },
    ])
  })

  it('writes struct with sibling structs', async () => {
    const data = [
      { position: { x: 1, y: 2 }, size: { w: 10, h: 20 }, label: 'A' },
      { position: { x: 3, y: null }, size: null, label: 'B' },
      { position: null, size: { w: 5, h: 6 }, label: null },
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'rect', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'rect', repetition_type: 'OPTIONAL', num_children: 3 },
        { name: 'position', repetition_type: 'OPTIONAL', num_children: 2 },
        { name: 'x', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'y', repetition_type: 'OPTIONAL', type: 'INT32' },
        { name: 'size', repetition_type: 'OPTIONAL', num_children: 2 },
        { name: 'w', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'h', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'label', repetition_type: 'OPTIONAL', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { rect: { position: { x: 1, y: 2 }, size: { w: 10, h: 20 }, label: 'A' } },
      { rect: { position: { x: 3, y: null }, size: undefined, label: 'B' } },
      { rect: { position: undefined, size: { w: 5, h: 6 }, label: null } },
    ])
  })

  it('writes nested lists (list of lists)', async () => {
    const data = [
      [[1, 2], [3, 4, 5]],
      [[]],
      [],
      null,
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'matrix', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'matrix', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        { name: 'element', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        { name: 'element', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { matrix: [[1, 2], [3, 4, 5]] },
      { matrix: [[]] },
      { matrix: [] },
      { matrix: undefined },
    ])
  })

  it('writes map using Map object input', async () => {
    const data = [
      new Map([['a', 1], ['b', 2]]),
      new Map(),
      null,
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'counts', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'counts', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { counts: { a: 1, b: 2 } },
      { counts: {} },
      { counts: undefined },
    ])
  })

  it('writes map using array of entries input', async () => {
    const data = [
      [{ key: 'x', value: 10 }, { key: 'y', value: 20 }],
      [['a', 100], ['b', 200]], // tuple format
      [],
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'values', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'values', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { values: { x: 10, y: 20 } },
      { values: { a: 100, b: 200 } },
      { values: {} },
    ])
  })

  it('writes map with INT32 keys', async () => {
    const data = [
      new Map([[1, 'one'], [2, 'two']]),
      new Map([[3, 'three']]),
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'lookup', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'lookup', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { lookup: { 1: 'one', 2: 'two' } },
      { lookup: { 3: 'three' } },
    ])
  })

  it('writes map with INT64 keys', async () => {
    const data = [
      new Map([[1n, 'one'], [2n, 'two']]),
      new Map([[3n, 'three']]),
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'lookup', data }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'lookup', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'INT64' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { lookup: { 1: 'one', 2: 'two' } },
      { lookup: { 3: 'three' } },
    ])
  })

  it('throws error for missing required child in struct', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{
        name: 'person',
        data: [
          { name: 'Ada' },
          {}, // missing required name
        ],
      }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'person', repetition_type: 'OPTIONAL', num_children: 1 },
        { name: 'name', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
      ],
    })).toThrow('required value is undefined')
  })

  it('throws error for non-array list field', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'items', data: ['not-an-array'] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'items', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        { name: 'element', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })).toThrow('must be an array')
  })

  it('throws error for non-object struct field', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'point', data: [42] }], // should be an object
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'point', repetition_type: 'OPTIONAL', num_children: 2 },
        { name: 'x', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'y', repetition_type: 'REQUIRED', type: 'INT32' },
      ],
    })).toThrow('must be an object')
  })

  it('throws error for array struct field', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'point', data: [[1, 2]] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'point', repetition_type: 'OPTIONAL', num_children: 2 },
        { name: 'x', repetition_type: 'OPTIONAL', type: 'INT32' },
        { name: 'y', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })).toThrow('must be an object')
  })

  it('throws error for invalid map entry format', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'data', data: [[{ invalid: 'entry' }]] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'data', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'BYTE_ARRAY', converted_type: 'UTF8' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })).toThrow('must provide key and value')
  })

  it('throws error for invalid map key type', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'data', data: [{ notANumber: 1 }] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'data', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })).toThrow('parquet expected integer value')
  })

  it('throws error for non-integer INT32 map key', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'data', data: [{ 1.5: 10 }] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'data', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })).toThrow('parquet expected integer value')
  })

  it('throws error for out-of-range INT32 map key', () => {
    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'data', data: [{ 2147483648: 1 }] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'data', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'MAP' },
        { name: 'key_value', repetition_type: 'REPEATED', num_children: 2 },
        { name: 'key', repetition_type: 'REQUIRED', type: 'INT32' },
        { name: 'value', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
    })).toThrow('parquet expected integer value')
  })
})
