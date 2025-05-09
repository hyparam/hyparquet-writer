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
      {
        name: 'timestamps',
        data: [new Date(1000000), new Date(2000000), new Date(3000000)],
        type: 'TIMESTAMP',
      },
    ] })
    const metadata = parquetMetadata(file)
    expect(metadata.schema).toEqual([
      {
        name: 'root',
        num_children: 1,
      },
      {
        converted_type: 'TIMESTAMP_MILLIS',
        name: 'timestamps',
        repetition_type: 'OPTIONAL',
        type: 'INT64',
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
      schema: schemaFromColumnData({
        columnData,
        schemaOverrides: {
          numbers: {
            name: 'numbers',
            type: 'DOUBLE',
            repetition_type: 'OPTIONAL',
            field_id: 1,
          },
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
