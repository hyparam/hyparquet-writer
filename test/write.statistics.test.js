import { parquetMetadataAsync, parquetQuery, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * @import {BasicType, ColumnSource} from '../src/types.js'
 * @import {Statistics} from 'hyparquet'
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
