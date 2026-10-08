import { parquetMetadataAsync, parquetQuery, parquetReadObjects } from 'hyparquet'
import { readColumnIndex } from 'hyparquet/src/indexes.js'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * @import {BasicType, ColumnSource} from '../src/types.js'
 * @import {Encoding, Statistics} from 'hyparquet'
 */

// A value longer than the 16-byte statistics truncation threshold.
const LONG = 'this-is-a-very-long-string-value-exceeding-sixteen-bytes' // 56 bytes
const UUID = '8ad1f570-bb0c-4ad0-9b57-4ad7d2d0f32b' // 36 bytes

/**
 * @param {any[]} data
 * @param {BasicType} [type]
 * @param {Partial<ColumnSource>} [extra]
 * @returns {ArrayBuffer}
 */
function writeCol(data, type = 'STRING', extra = {}) {
  return parquetWriteBuffer({
    columnData: [{ name: 'col', data, type, ...extra }],
    statistics: true,
  })
}

/**
 * @param {ArrayBuffer} buffer
 * @returns {Promise<Statistics>}
 */
async function readStats(buffer) {
  const meta = await parquetMetadataAsync(buffer)
  const stats = meta.row_groups[0].columns[0].meta_data?.statistics
  if (!stats) throw new Error('expected statistics')
  return stats
}

describe('statistics truncation of long string values', () => {
  it('min_value is a valid lower bound and max_value a valid upper bound', async () => {
    const stats = await readStats(writeCol([LONG]))
    // The core invariant the reader relies on for predicate pushdown:
    // every value in the row group must satisfy min <= value <= max.
    expect(String(stats.min_value) <= LONG).toBe(true)
    expect(String(stats.max_value) >= LONG).toBe(true)
  })

  it('finds a single long value with an exact-match query', async () => {
    const buffer = writeCol([LONG])
    const rows = await parquetQuery({ file: buffer, filter: { col: { $eq: LONG } } })
    expect(rows.map(r => r.col)).toEqual([LONG])
  })

  it('finds a long max value in a multi-value column via $eq', async () => {
    const buffer = writeCol(['apple', 'banana', LONG])
    const rows = await parquetQuery({ file: buffer, filter: { col: { $eq: LONG } } })
    expect(rows.map(r => r.col)).toEqual([LONG])
  })

  it('finds a long max value via a $gte range query', async () => {
    const buffer = writeCol(['apple', 'banana', LONG])
    const rows = await parquetQuery({ file: buffer, filter: { col: { $gte: LONG } } })
    expect(rows.map(r => r.col)).toEqual([LONG])
  })

  it('marks truncated bounds as inexact', async () => {
    const stats = await readStats(writeCol([LONG]))
    expect(stats.is_min_value_exact).toBe(false)
    expect(stats.is_max_value_exact).toBe(false)
  })

  it('leaves short values untruncated and exact', async () => {
    const stats = await readStats(writeCol(['hello']))
    expect(stats.min_value).toBe('hello')
    expect(stats.max_value).toBe('hello')
    // not flagged inexact (true or omitted are both acceptable, false is not)
    expect(stats.is_min_value_exact).not.toBe(false)
    expect(stats.is_max_value_exact).not.toBe(false)
  })

  it('finds a long value even with page-level column index enabled', async () => {
    // Multiple pages; the long value lives in a later page so a wrong
    // page-level max would cause the page to be skipped.
    const data = Array.from({ length: 50 }, (_, i) => `row-${i}`)
    data.push(LONG)
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data, type: 'STRING', columnIndex: true }],
      statistics: true,
      pageSize: 100, // force multiple pages
    })
    const rows = await parquetQuery({ file: buffer, filter: { col: { $eq: LONG } } })
    expect(rows.map(r => r.col)).toEqual([LONG])
  })
})

describe('statistics for UUID columns', () => {
  it('encodes UUID min/max as the raw 16 bytes, not ASCII text', async () => {
    // hyparquet now decodes UUID statistics back to a string. Asserting the
    // decoded min/max equal the true UUIDs proves the writer stored the value's
    // genuine 16-byte big-endian form (an ASCII encoding would decode to junk).
    const stats = await readStats(writeCol(['00000000-0000-0000-0000-000000000000', UUID], 'UUID'))
    expect(stats.min_value).toBe('00000000-0000-0000-0000-000000000000')
    expect(stats.max_value).toBe(UUID)
  })

  it('round-trips UUID column data', async () => {
    const buffer = writeCol([UUID], 'UUID')
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows[0].col).toBe(UUID)
  })
})

describe('statistics for DECIMAL columns', () => {
  /** @type {import('hyparquet').SchemaElement[]} */
  const schema = [
    { name: 'root', num_children: 1 },
    { name: 'col', type: 'INT64', converted_type: 'DECIMAL', precision: 18, scale: 2 },
  ]

  it('normalizes mixed number and bigint representations before comparison', async () => {
    // 200n is the unscaled representation of 2.00; 10 is the logical value 10.00.
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data: [200n, 10] }],
      schema,
      statistics: true,
    })
    const stats = await readStats(buffer)
    expect(stats.min_value).toBe(2)
    expect(stats.max_value).toBe(10)

    const rows = await parquetQuery({ file: buffer, filter: { col: { $eq: 2 } } })
    expect(rows).toEqual([{ col: 2 }])
  })

  it('normalizes mixed representations in page indexes', async () => {
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data: [200n, 10, 300n], columnIndex: true }],
      schema,
      statistics: false, // isolate page-index pruning from row-group statistics
      pageSize: 24,
    })
    const rows = await parquetQuery({
      file: buffer,
      filter: { col: { $eq: 2 } },
      usePageIndex: true,
    })
    expect(rows).toEqual([{ col: 2 }])
  })

  it('writes INT32 decimal statistics as int32, not float32', async () => {
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'price', data: [123.45, -0.5] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'price', type: 'INT32', repetition_type: 'OPTIONAL', converted_type: 'DECIMAL', precision: 6, scale: 2 },
      ],
      statistics: true,
    })
    const stats = await readStats(buffer)
    expect(stats.min_value).toBe(-0.5)
    expect(stats.max_value).toBe(123.45)
  })
})
describe('statistics for non-BMP strings', () => {
  // UTF-8 byte order (code point order) puts U+E000 before U+10000, but
  // JavaScript UTF-16 comparison puts the surrogate pair for U+10000 first.
  const BMP = ''
  const ASTRAL = '\u{10000}'

  it('uses UTF-8 byte order for min/max with plain encoding', async () => {
    const stats = await readStats(writeCol([ASTRAL, BMP], 'STRING', { encoding: 'PLAIN' }))
    expect(stats.min_value).toBe(BMP)
    expect(stats.max_value).toBe(ASTRAL)
  })

  it('uses UTF-8 byte order for min/max with dictionary encoding', async () => {
    const buffer = writeCol([ASTRAL, BMP, ASTRAL, BMP], 'STRING', { encoding: 'RLE_DICTIONARY' })
    const meta = await parquetMetadataAsync(buffer)
    expect(meta.row_groups[0].columns[0].meta_data?.encodings).toContain('RLE_DICTIONARY')
    const stats = await readStats(buffer)
    expect(stats.min_value).toBe(BMP)
    expect(stats.max_value).toBe(ASTRAL)
  })

  it('ignores nulls when computing min/max', async () => {
    const stats = await readStats(writeCol([null, ASTRAL, null, BMP, null]))
    expect(stats.min_value).toBe(BMP)
    expect(stats.max_value).toBe(ASTRAL)
    expect(stats.null_count).toBe(3n)
  })

  it('orders strings sharing a prefix and lone surrogates like their UTF-8 bytes', async () => {
    const data = ['a\u{10000}', 'az', 'a\uD800', 'a�', 'a']
    const stats = await readStats(writeCol(data))
    expect(stats.min_value).toBe('a')
    expect(stats.max_value).toBe('a\u{10000}')
  })

  /** @type {Encoding[]} */
  const encodings = ['PLAIN', 'RLE_DICTIONARY']
  for (const encoding of encodings) {
    it(`finds rows with $eq and $in using ${encoding} encoding`, async () => {
      const buffer = writeCol([ASTRAL, null, BMP], 'STRING', { encoding })
      const eqBmp = await parquetQuery({ file: buffer, filter: { col: { $eq: BMP } } })
      expect(eqBmp.map(r => r.col)).toEqual([BMP])
      const eqAstral = await parquetQuery({ file: buffer, filter: { col: { $eq: ASTRAL } } })
      expect(eqAstral.map(r => r.col)).toEqual([ASTRAL])
      const inRows = await parquetQuery({ file: buffer, filter: { col: { $in: [BMP, ASTRAL] } } })
      expect(inRows.map(r => r.col).sort()).toEqual([ASTRAL, BMP].sort())
    })
  }

  it('writes page index bounds and boundary order in UTF-8 byte order', async () => {
    // Pages ascend in code point order but descend in UTF-16 order
    const data = [
      ...Array(20).fill(BMP),
      ...Array(20).fill(ASTRAL),
      ...Array(20).fill(null),
    ]
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data, type: 'STRING', encoding: 'PLAIN', columnIndex: true }],
      statistics: true,
      pageSize: 40,
    })
    const meta = await parquetMetadataAsync(buffer)
    const column = meta.row_groups[0].columns[0]
    const reader = {
      view: new DataView(buffer, Number(column.column_index_offset), column.column_index_length),
      offset: 0,
    }
    const columnIndex = readColumnIndex(reader, meta.schema[1])
    expect(columnIndex.null_pages).toEqual([false, false, false, false])
    expect(columnIndex.min_values).toEqual([BMP, BMP, ASTRAL, ASTRAL])
    expect(columnIndex.max_values).toEqual([BMP, ASTRAL, ASTRAL, ASTRAL])
    expect(columnIndex.boundary_order).toBe('ASCENDING')

    for (const value of [BMP, ASTRAL]) {
      const rows = await parquetQuery({ file: buffer, filter: { col: { $eq: value } } })
      expect(rows.length).toBe(20)
      expect(rows.every(r => r.col === value)).toBe(true)
    }
  })
})

describe('statistics for number INT64 values', () => {
  it('encodes number bounds of a DELTA_BINARY_PACKED INT64 column as int64', async () => {
    const stats = await readStats(writeCol([3, -5, 2 ** 40], 'INT64', { encoding: 'DELTA_BINARY_PACKED' }))
    expect(stats.min_value).toBe(-5n)
    expect(stats.max_value).toBe(2n ** 40n)
  })

  it('encodes number bounds of a TIMESTAMP column as int64', async () => {
    const buffer = writeCol([1000, 3000, 2000], 'TIMESTAMP')
    const stats = await readStats(buffer)
    expect(stats.min_value).toEqual(new Date(1000))
    expect(stats.max_value).toEqual(new Date(3000))
    const rows = await parquetQuery({ file: buffer, filter: { col: { $gt: new Date(2500) } } })
    expect(rows).toEqual([{ col: new Date(3000) }])
  })
})

describe('statistics for FLOAT16 columns', () => {
  it('encodes bounds as float16, not text', async () => {
    const buffer = writeCol([1.5, -2, NaN, null], 'FLOAT16')
    const stats = await readStats(buffer)
    expect(stats.min_value).toBe(-2)
    expect(stats.max_value).toBe(1.5)
    expect(stats.is_min_value_exact).toBeUndefined()
    expect(stats.is_max_value_exact).toBeUndefined()
    const rows = await parquetQuery({ file: buffer, filter: { col: { $gt: 1 } } })
    expect(rows).toEqual([{ col: 1.5 }])
  })

  it('encodes page index bounds as float16', async () => {
    const data = Array.from({ length: 40 }, (_, i) => i / 4)
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data, type: 'FLOAT16', columnIndex: true }],
      statistics: false,
      pageSize: 20,
    })
    const rows = await parquetQuery({ file: buffer, filter: { col: { $gte: 9.5 } }, usePageIndex: true })
    expect(rows.map(r => r.col)).toEqual([9.5, 9.75])
  })
})

describe('statistics for Date values', () => {
  it('writes bounds for a column of Dates', async () => {
    const buffer = writeCol([new Date(5000), new Date(1000), null], 'TIMESTAMP')
    const stats = await readStats(buffer)
    expect(stats.min_value).toEqual(new Date(1000))
    expect(stats.max_value).toEqual(new Date(5000))
    const rows = await parquetQuery({ file: buffer, filter: { col: { $gt: new Date(2000) } } })
    expect(rows).toEqual([{ col: new Date(5000) }])
  })

  it('compares Dates and bigints in the column unit', async () => {
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data: [new Date(20), 1000n, 9000n] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'col', type: 'INT64', converted_type: 'TIMESTAMP_MICROS', repetition_type: 'OPTIONAL' },
      ],
      statistics: true,
    })
    const stats = await readStats(buffer)
    expect(stats.min_value).toEqual(new Date(1))
    expect(stats.max_value).toEqual(new Date(20))
    const rows = await parquetQuery({ file: buffer, filter: { col: { $gt: new Date(10) } } })
    expect(rows).toEqual([{ col: new Date(20) }])
  })

  it('writes DATE bounds in days', async () => {
    const day = 86400000
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'col', data: [new Date(3 * day), 1, new Date(2 * day)] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'col', type: 'INT32', converted_type: 'DATE', repetition_type: 'OPTIONAL' },
      ],
      statistics: true,
    })
    const stats = await readStats(buffer)
    expect(stats.min_value).toEqual(new Date(day))
    expect(stats.max_value).toEqual(new Date(3 * day))
  })
})
