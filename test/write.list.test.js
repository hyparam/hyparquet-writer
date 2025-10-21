import { parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

describe('parquetWrite lists', () => {
  it('writes optional list columns', async () => {
    const listy = [
      [1, 2],
      null,
      [],
      [3, null, 4],
      [null],
    ]

    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'listy', data: listy }],
      schema: [
        { name: 'root', num_children: 1 },
        {
          name: 'listy',
          repetition_type: 'OPTIONAL',
          num_children: 1,
          converted_type: 'LIST',
        },
        {
          name: 'list',
          repetition_type: 'REPEATED',
          num_children: 1,
        },
        {
          name: 'element',
          repetition_type: 'OPTIONAL',
          type: 'INT32',
        },
      ],
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toEqual([
      { listy: [1, 2] },
      { listy: undefined },
      { listy: [] },
      { listy: [3, null, 4] },
      { listy: [null] },
    ])
  })

  it('throws on null data for required list columns', () => {
    /**
     * Schema for a required list of required INT32 values.
     * @type {import('hyparquet').SchemaElement[]}
     */
    const requiredListSchema = [
      { name: 'root', num_children: 1 },
      {
        name: 'numbers',
        repetition_type: 'REQUIRED',
        num_children: 1,
        converted_type: 'LIST',
      },
      {
        name: 'list',
        repetition_type: 'REPEATED',
        num_children: 1,
      },
      {
        name: 'element',
        repetition_type: 'REQUIRED',
        type: 'INT32',
      },
    ]

    expect(() => parquetWriteBuffer({
      columnData: [{ name: 'numbers', data: [[420], null] }],
      schema: requiredListSchema,
    })).toThrow('parquet required value is undefined')
  })
})
