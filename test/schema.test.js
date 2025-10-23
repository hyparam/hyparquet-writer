import { describe, expect, it } from 'vitest'
import { autoSchemaElement, getMaxDefinitionLevel, getMaxRepetitionLevel, schemaFromColumnData } from '../src/schema.js'

/**
 * @import {SchemaElement} from 'hyparquet'
 */

describe('schemaFromColumnData', () => {
  it('honours provided type with nullable = false → REQUIRED', () => {
    const schema = schemaFromColumnData({
      columnData: [
        { name: 'id', data: new Int32Array([1, 2, 3]), type: 'INT32', nullable: false },
      ],
    })
    expect(schema[1]).toEqual({ name: 'id', type: 'INT32', repetition_type: 'REQUIRED' })
  })

  it('applies valid schema override verbatim', () => {
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
    expect(schema[1].name).toBe('strings')
    expect(schema[1].type).toBe('BYTE_ARRAY')
    expect(schema[1].converted_type).toBe('UTF8')
    expect(schema[1].repetition_type).toBe('OPTIONAL')
  })

  it('throws when column lengths differ', () => {
    expect(() =>
      schemaFromColumnData({
        columnData: [
          { name: 'a', data: new Int32Array([1]) },
          { name: 'b', data: new Int32Array([1, 2]) },
        ],
      })
    ).toThrow(/columns must have the same length/)
  })

  it('rejects override with mismatched name', () => {
    expect(() =>
      schemaFromColumnData({
        columnData: [{ name: 'x', data: new Int32Array([1]) }],
        schemaOverrides: { x: { name: 'y', type: 'INT32' } },
      })
    ).toThrow(/does not match column name/)
  })
})

describe('autoSchemaElement', () => {
  it.each([
    [new Int32Array([1, 2]), 'INT32'],
    [new BigInt64Array([1n, 2n]), 'INT64'],
    [new Float32Array([1, 2]), 'FLOAT'],
    [new Float64Array([1, 2]), 'DOUBLE'],
  ])('detects typed arrays (%#)', (data, expected) => {
    const el = autoSchemaElement('col', data)
    expect(el.type).toBe(expected)
    expect(el.repetition_type).toBe('REQUIRED')
  })

  it('promotes INT32 + DOUBLE mix to DOUBLE', () => {
    const el = autoSchemaElement('mix', [1, 2.5])
    expect(el.type).toBe('DOUBLE')
  })

  it('sets repetition_type OPTIONAL when nulls present', () => {
    const el = autoSchemaElement('maybe', [null, 1])
    expect(el.repetition_type).toBe('OPTIONAL')
  })

  it('falls back to BYTE_ARRAY for empty arrays', () => {
    const el = autoSchemaElement('empty', [])
    expect(el.type).toBe('BYTE_ARRAY')
    expect(el.repetition_type).toBe('OPTIONAL')
  })

  it('throws on incompatible mixed scalar types', () => {
    expect(() => autoSchemaElement('bad', [1, 'a'])).toThrow(/mixed types/)
  })
})

describe('level helpers', () => {
  /** @type {SchemaElement[]} */
  const path = [
    { name: 'root', repetition_type: 'REPEATED' },
    { name: 'child', repetition_type: 'OPTIONAL' },
    { name: 'leaf', repetition_type: 'REPEATED' },
  ]

  it('computes max repetition level', () => {
    expect(getMaxRepetitionLevel(path)).toBe(2)
  })

  it('computes max definition level', () => {
    expect(getMaxDefinitionLevel(path)).toBe(2)
  })
})
