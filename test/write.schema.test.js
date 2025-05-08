import { parquetMetadata } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer, schemaFromColumnData } from '../src/index.js'

describe('parquet schema', () => {
  it('auto detects types', () => {
    const file = parquetWriteBuffer({ columnData: [
      { name: 'strings', data: ['1', '2', '3'] },
    ] })
    const metadata = parquetMetadata(file)
    expect(metadata.schema).toEqual([
      {
        name: 'root',
        num_children: 1,
      },
      {
        converted_type: 'UTF8',
        name: 'strings',
        repetition_type: 'REQUIRED',
        type: 'BYTE_ARRAY',
      },
    ])
  })

  it('accepts basic type hints', () => {
    const file = parquetWriteBuffer({ columnData: [
      { name: 'numbers', data: [1, 2, 3], type: 'FLOAT' },
    ] })
    const metadata = parquetMetadata(file)
    expect(metadata.schema).toEqual([
      {
        name: 'root',
        num_children: 1,
      },
      {
        name: 'numbers',
        repetition_type: 'OPTIONAL',
        type: 'FLOAT',
      },
    ])
  })

  it('accepts nullable basic type hints', () => {
    const file = parquetWriteBuffer({ columnData: [
      { name: 'numbers', data: [1, 2, 3], type: 'FLOAT', nullable: false },
    ] })
    const metadata = parquetMetadata(file)
    expect(metadata.schema).toEqual([
      {
        converted_type: undefined,
        field_id: undefined,
        logical_type: undefined,
        name: 'root',
        num_children: 1,
        precision: undefined,
        repetition_type: undefined,
        scale: undefined,
        type: undefined,
        type_length: undefined,
      },
      {
        converted_type: undefined,
        field_id: undefined,
        logical_type: undefined,
        name: 'numbers',
        num_children: undefined,
        precision: undefined,
        repetition_type: 'REQUIRED',
        scale: undefined,
        type: 'FLOAT',
        type_length: undefined,
      },
    ])
  })

  it('accepts explicit schema', () => {
    const file = parquetWriteBuffer({ columnData: [
      { name: 'numbers', data: [1, 2, 3] },
    ], schema: [
      { name: 'root', num_children: 1 },
      { name: 'numbers', type: 'FLOAT', repetition_type: 'REQUIRED' },
    ] })
    const metadata = parquetMetadata(file)
    expect(metadata.schema).toEqual([
      {
        name: 'root',
        num_children: 1,
      },
      {
        name: 'numbers',
        repetition_type: 'REQUIRED',
        type: 'FLOAT',
      },
    ])
  })

  it('accepts schema override', () => {
    const columnData = [
      { name: 'numbers', data: [1, 2, 3] },
    ]
    const file = parquetWriteBuffer({
      columnData,
      schema: schemaFromColumnData(columnData, {
        numbers: {
          name: 'numbers',
          type: 'DOUBLE',
          repetition_type: 'OPTIONAL',
          field_id: 1,
        },
      }),
    })
    const metadata = parquetMetadata(file)
    expect(metadata.schema).toEqual([
      {
        name: 'root',
        num_children: 1,
      },
      {
        field_id: 1,
        name: 'numbers',
        repetition_type: 'OPTIONAL',
        type: 'DOUBLE',
      },
    ])
  })

  it('throws if basic types conflict with schema', () => {
    expect(() => {
      parquetWriteBuffer({
        columnData: [
          { name: 'numbers', data: [1, 2, 3], type: 'FLOAT' },
        ],
        schema: [
          { name: 'root', num_children: 1 },
          { name: 'numbers', type: 'DOUBLE', repetition_type: 'OPTIONAL' },
        ],
      })
    }).toThrow('cannot provide both schema and columnData type')
  })
})
