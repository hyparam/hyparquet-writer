import { describe, expect, it } from 'vitest'
import { autoSchemaElement, getMaxRepetitionLevel, schemaFromColumnData } from '../src/schema.js'

describe('schemaFromColumnData', () => {
  describe('basic types', () => {
    it('repetition REQUIRED when nullable false', () => {
      const schema = schemaFromColumnData({
        columnData: [
          { name: 'id', data: new Int32Array([1, 2, 3]), type: 'INT32', nullable: false },
        ],
      })
      expect(schema[0]).toEqual({ name: 'root', num_children: 1 })
      expect(schema[1]).toEqual({ name: 'id', type: 'INT32', repetition_type: 'REQUIRED' })
    })

    it('repetition OPTIONAL when nullable is not specified', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'x', data: [1], type: 'DOUBLE' }],
      })
      expect(schema[1]).toEqual({ name: 'x', type: 'DOUBLE', repetition_type: 'OPTIONAL' })
    })

    it('maps STRING to BYTE_ARRAY UTF8', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 's', data: ['hi'], type: 'STRING' }],
      })
      expect(schema[1]).toEqual({
        name: 's', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL',
      })
    })

    it('maps JSON to BYTE_ARRAY JSON', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'j', data: [{}], type: 'JSON' }],
      })
      expect(schema[1]).toEqual({
        name: 'j', type: 'BYTE_ARRAY', converted_type: 'JSON', repetition_type: 'OPTIONAL',
      })
    })

    it('maps TIMESTAMP to INT64 TIMESTAMP_MILLIS', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 't', data: [new Date()], type: 'TIMESTAMP' }],
      })
      expect(schema[1]).toEqual({
        name: 't', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS', repetition_type: 'OPTIONAL',
      })
    })

    it('maps UUID to FIXED_LEN_BYTE_ARRAY length 16', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'u', data: ['x'], type: 'UUID' }],
      })
      expect(schema[1]).toEqual({
        name: 'u', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 16,
        logical_type: { type: 'UUID' }, repetition_type: 'OPTIONAL',
      })
    })

    it('maps FLOAT16 to FIXED_LEN_BYTE_ARRAY length 2', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'f', data: [1.0], type: 'FLOAT16' }],
      })
      expect(schema[1]).toEqual({
        name: 'f', type: 'FIXED_LEN_BYTE_ARRAY', type_length: 2,
        logical_type: { type: 'FLOAT16' }, repetition_type: 'OPTIONAL',
      })
    })

    it('maps GEOMETRY to BYTE_ARRAY with logical type', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'g', data: [new Uint8Array()], type: 'GEOMETRY' }],
      })
      expect(schema[1]).toEqual({
        name: 'g', type: 'BYTE_ARRAY', logical_type: { type: 'GEOMETRY' }, repetition_type: 'OPTIONAL',
      })
    })

    it('maps GEOGRAPHY to BYTE_ARRAY with logical type', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'g', data: [new Uint8Array()], type: 'GEOGRAPHY' }],
      })
      expect(schema[1]).toEqual({
        name: 'g', type: 'BYTE_ARRAY', logical_type: { type: 'GEOGRAPHY' }, repetition_type: 'OPTIONAL',
      })
    })
  })

  describe('detect types', () => {
    it('auto-detects strings', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 's', data: ['a', 'b'] }],
      })
      expect(schema[1]).toEqual({
        name: 's', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED',
      })
    })

    it('auto-detects integers', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'n', data: [1, 2, 3] }],
      })
      expect(schema[1]).toEqual({ name: 'n', type: 'INT32', repetition_type: 'REQUIRED' })
    })

    it('auto-detects doubles with type expansion', () => {
      const schema = schemaFromColumnData({
        columnData: [
          { name: 'f', data: [1, 2.5, 3] },
          { name: 'nan', data: [NaN, Infinity, -Infinity] },
        ],
      })
      expect(schema[1]).toEqual({ name: 'f', type: 'DOUBLE', repetition_type: 'REQUIRED' })
      expect(schema[2]).toEqual({ name: 'nan', type: 'DOUBLE', repetition_type: 'REQUIRED' })
    })

    it('auto-detects dates', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'd', data: [new Date()] }],
      })
      expect(schema[1]).toEqual({
        name: 'd', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS', repetition_type: 'REQUIRED',
      })
    })

    it('auto-detects objects as json', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'o', data: [{ a: 1 }] }],
      })
      expect(schema[1]).toEqual({
        name: 'o', type: 'BYTE_ARRAY', converted_type: 'JSON', repetition_type: 'REQUIRED',
      })
    })
  })

  describe('schema overrides', () => {
    it('applies valid schema override', () => {
      const schema = schemaFromColumnData({
        columnData: [{ name: 'strings', data: ['a', 'b'] }],
        schemaOverrides: {
          strings: {
            name: 'strings',
            type: 'BYTE_ARRAY',
            converted_type: 'UTF8',
            repetition_type: 'OPTIONAL',
          },
        },
      })
      expect(schema[0]).toEqual({ name: 'root', num_children: 1 })
      expect(schema[1]).toEqual({
        name: 'strings',
        type: 'BYTE_ARRAY',
        converted_type: 'UTF8',
        repetition_type: 'OPTIONAL',
      })
    })

    it('rejects override with mismatched name', () => {
      expect(() =>
        schemaFromColumnData({
          columnData: [{ name: 'x', data: [1] }],
          schemaOverrides: { x: { name: 'y', type: 'INT32' } },
        })
      ).toThrow('schema override for column x must have matching name, got y')
    })

    it('rejects override with basic type specified', () => {
      expect(() =>
        schemaFromColumnData({
          columnData: [{ name: 'x', data: [1], type: 'DOUBLE' }],
          schemaOverrides: { x: { name: 'x', type: 'INT32' } },
        })
      ).toThrow('cannot provide both type and schema override for column x')
    })

    it('rejects override with nullable specified', () => {
      expect(() =>
        schemaFromColumnData({
          columnData: [{ name: 'x', data: [1], nullable: false }],
          schemaOverrides: { x: { name: 'x', type: 'INT32' } },
        })
      ).toThrow('cannot provide both type and schema override for column x')
    })

    it('rejects FIXED_LEN_BYTE_ARRAY override without type_length', () => {
      expect(() =>
        schemaFromColumnData({
          columnData: [{ name: 'hash', data: [new Uint8Array(16)] }],
          schemaOverrides: { hash: { name: 'hash', type: 'FIXED_LEN_BYTE_ARRAY' } },
        })
      ).toThrow('schema override for FIXED_LEN_BYTE_ARRAY must include type_length')
    })

    it('rejects override with num_children (nested types not supported)', () => {
      expect(() =>
        schemaFromColumnData({
          columnData: [{ name: 'nested', data: [[1, 2]] }],
          schemaOverrides: { nested: { name: 'nested', type: 'INT32', num_children: 1 } },
        })
      ).toThrow('schema override does not support nested types')
    })
  })
})

describe('autoSchemaElement', () => {
  describe('typed arrays', () => {
    it('detects typed arrays', () => {
      expect(autoSchemaElement('col', new Int32Array([1, 2])))
        .toEqual({ name: 'col', type: 'INT32', repetition_type: 'REQUIRED' })
      expect(autoSchemaElement('col', new BigInt64Array([1n, 2n])))
        .toEqual({ name: 'col', type: 'INT64', repetition_type: 'REQUIRED' })
      expect(autoSchemaElement('col', new Float32Array([1, 2])))
        .toEqual({ name: 'col', type: 'FLOAT', repetition_type: 'REQUIRED' })
      expect(autoSchemaElement('col', new Float64Array([1, 2])))
        .toEqual({ name: 'col', type: 'DOUBLE', repetition_type: 'REQUIRED' })
    })
  })

  describe('type detection', () => {
    it('detects booleans', () => {
      expect(autoSchemaElement('b', [true, false]))
        .toEqual({ name: 'b', type: 'BOOLEAN', repetition_type: 'REQUIRED' })
    })

    it('detects bigints as INT64', () => {
      expect(autoSchemaElement('bi', [1n, 2n]))
        .toEqual({ name: 'bi', type: 'INT64', repetition_type: 'REQUIRED' })
    })

    it('detects integers as INT32', () => {
      expect(autoSchemaElement('maybe', [null, 1]))
        .toEqual({ name: 'maybe', type: 'INT32', repetition_type: 'OPTIONAL' })
    })

    it('promotes INT32 + DOUBLE mix to DOUBLE', () => {
      expect(autoSchemaElement('mix', [1, 2.5]))
        .toEqual({ name: 'mix', type: 'DOUBLE', repetition_type: 'REQUIRED' })
    })

    it('promotes DOUBLE then INT32 to DOUBLE', () => {
      expect(autoSchemaElement('mix', [2.5, 1]))
        .toEqual({ name: 'mix', type: 'DOUBLE', repetition_type: 'REQUIRED' })
    })

    it('detects Uint8Array as BYTE_ARRAY', () => {
      expect(autoSchemaElement('bin', [new Uint8Array([1]), new Uint8Array([2])]))
        .toEqual({ name: 'bin', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED' })
    })

    it('detects strings as BYTE_ARRAY UTF8', () => {
      expect(autoSchemaElement('s', ['a', 'b']))
        .toEqual({ name: 's', type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'REQUIRED' })
    })

    it('detects dates as INT64 TIMESTAMP_MILLIS', () => {
      expect(autoSchemaElement('d', [new Date('2024-01-01'), new Date('2024-06-01')]))
        .toEqual({ name: 'd', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS', repetition_type: 'REQUIRED' })
    })

    it('detects objects as BYTE_ARRAY JSON', () => {
      expect(autoSchemaElement('o', [{ a: 1 }, { b: 2 }]))
        .toEqual({ name: 'o', type: 'BYTE_ARRAY', converted_type: 'JSON', repetition_type: 'REQUIRED' })
    })

    it('sets repetition_type OPTIONAL when nulls present', () => {
      expect(autoSchemaElement('maybe', [null, 1]))
        .toEqual({ name: 'maybe', type: 'INT32', repetition_type: 'OPTIONAL' })
    })

    it('defaults to optional BYTE_ARRAY for empty arrays', () => {
      expect(autoSchemaElement('empty', []))
        .toEqual({ name: 'empty', type: 'BYTE_ARRAY', repetition_type: 'OPTIONAL' })
    })

    it('returns OPTIONAL for all-null arrays', () => {
      expect(autoSchemaElement('n', [null, undefined]))
        .toEqual({ name: 'n', type: 'BYTE_ARRAY', repetition_type: 'OPTIONAL' })
    })
  })

  describe('invalid schemas', () => {
    it('throws on incompatible mixed scalar types', () => {
      expect(() => autoSchemaElement('bad', [1, 'a']))
        .toThrow('parquet cannot write mixed types: INT32 and UTF8')
    })

    it('throws on mixed bigints and numbers', () => {
      expect(() => autoSchemaElement('bad', [1n, 2]))
        .toThrow('parquet cannot write mixed types: INT64 and INT32')
    })

    it('throws on mixed dates and numbers', () => {
      expect(() => autoSchemaElement('bad', [new Date(), 1]))
        .toThrow('parquet cannot write mixed types: TIMESTAMP_MILLIS and INT32')
    })

    it('throws on non-widened mixed types', () => {
      expect(() => autoSchemaElement('bad', [true, 1n]))
        .toThrow('parquet cannot write mixed types: BOOLEAN and INT64')
    })

    it('throws on mixed strings and bytes', () => {
      expect(() => autoSchemaElement('bad', ['a', new Uint8Array([1])]))
        .toThrow('parquet cannot write mixed types: UTF8 and BYTE_ARRAY')
    })

    it('throws on mixed bytes and strings', () => {
      expect(() => autoSchemaElement('bad', [new Uint8Array([1]), 'a']))
        .toThrow('parquet cannot write mixed types: BYTE_ARRAY and UTF8')
    })

    it('throws on mixed dates and bigints', () => {
      expect(() => autoSchemaElement('bad', [1n, new Date()]))
        .toThrow('parquet cannot write mixed types: INT64 and TIMESTAMP_MILLIS')
    })
  })
})

describe('max repetition level helper', () => {
  it('flat column', () => {
    expect(getMaxRepetitionLevel([
      { name: 'root' },
      { name: 'leaf', repetition_type: 'OPTIONAL' },
    ])).toBe(0)
  })

  it('nested column', () => {
    expect(getMaxRepetitionLevel([
      { name: 'root' },
      { name: 'child', repetition_type: 'OPTIONAL' },
      { name: 'leaf', repetition_type: 'REPEATED' },
    ])).toBe(1)
  })
})
