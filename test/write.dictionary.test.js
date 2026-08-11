import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * @import {ColumnSource} from '../src/types.js'
 */

/**
 * Untyped BYTE_ARRAY columns read back as a string; normalize to bytes.
 * @param {any} v
 * @returns {any}
 */
function toBytes(v) {
  return typeof v === 'string' ? Uint8Array.from(v, c => c.charCodeAt(0)) : v
}

/** FNV-1a hash, mirroring useDictionary's byte-array bucketing in src/column.js.
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function hashBytes(bytes) {
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]
    h = Math.imul(h, 0x01000193)
  }
  return h >>> 0
}

describe('parquetWrite dictionary encoding', () => {
  it('dedupes byte-array values by content, not object identity', async () => {
    // 500 distinct Uint8Array objects that all hold identical bytes.
    // These should collapse to a single dictionary entry, but a Set/Map keyed
    // on object identity sees 500 unique values and falls back to PLAIN.
    const numRows = 500
    function bytes() { return new Uint8Array(2000).fill(7) }
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'blob', data: Array.from({ length: numRows }, bytes), type: 'BYTE_ARRAY' },
    ]

    const buffer = parquetWriteBuffer({ columnData })

    const metadata = parquetMetadata(buffer)
    const column = metadata.row_groups[0].columns[0]
    expect(column.meta_data?.encodings).toContain('RLE_DICTIONARY')

    // a single deduped 2000-byte entry should make the file tiny relative to
    // 500 * 2000 = 1,000,000 raw bytes
    expect(buffer.byteLength).toBeLessThan(10_000)

    // round-trips correctly (untyped BYTE_ARRAY reads back as a latin1 string)
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(toBytes(rows[0].blob)).toEqual(bytes())
    expect(toBytes(rows[499].blob)).toEqual(bytes())
  })

  it('keeps hash-colliding byte arrays distinct', async () => {
    // Two different byte sequences that share the same FNV-1a hash. The writer
    // buckets byte-array dictionary values by hash, so it must verify byte
    // equality before treating them as the same entry; otherwise a collision
    // would silently map one value onto the other. Bytes are kept < 128 so the
    // untyped BYTE_ARRAY round-trips losslessly as a string.
    function a() { return Uint8Array.of(27, 83, 52, 67, 82, 108, 98, 124) }
    function b() { return Uint8Array.of(25, 73, 7, 10, 109, 25, 4, 10) }
    expect(hashBytes(a())).toBe(hashBytes(b())) // genuinely collide
    expect(a()).not.toEqual(b()) // but are different values

    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'blob', data: Array.from({ length: numRows }, (_, i) => i % 2 ? b() : a()), type: 'BYTE_ARRAY' },
    ]

    const buffer = parquetWriteBuffer({ columnData })

    // low cardinality, so it is dictionary encoded (exercising the hash path)
    const column = parquetMetadata(buffer).row_groups[0].columns[0]
    expect(column.meta_data?.encodings).toContain('RLE_DICTIONARY')

    // both colliding values survive as distinct entries and round-trip per row
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    for (let i = 0; i < numRows; i++) {
      expect(toBytes(rows[i].blob)).toEqual(i % 2 ? b() : a())
    }
  })

  it('keeps the dictionary for low-cardinality large-value columns (#35)', async () => {
    // 40 distinct 30kb strings over 1200 rows: the distinct bytes (1.2mb)
    // exceed the old pageSize-coupled cap, but the dictionary replaces 36mb of
    // values, so it must be kept. This is the system-prompt-per-log-row shape
    // that ballooned real files 20-100x under the old fallback.
    const distinct = Array.from({ length: 40 }, (_, i) => String(i).padStart(6, '0').repeat(5000))
    const numRows = 1200
    const data = Array.from({ length: numRows }, (_, i) => distinct[i % 40])
    /** @type {ColumnSource[]} */
    const columnData = [{ name: 'prompt', data, type: 'STRING' }]

    const buffer = parquetWriteBuffer({ columnData })

    const metadata = parquetMetadata(buffer)
    for (const rg of metadata.row_groups) {
      expect(rg.columns[0].meta_data?.encodings).toContain('RLE_DICTIONARY')
    }

    // plain encoding of the same column is dramatically larger
    const plainBuffer = parquetWriteBuffer({ columnData: [{ ...columnData[0], encoding: 'PLAIN' }] })
    expect(buffer.byteLength * 5).toBeLessThan(plainBuffer.byteLength)

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0].prompt).toBe(distinct[0])
    expect(rows[numRows - 1].prompt).toBe(distinct[(numRows - 1) % 40])
  })

  it('keeps a winning dictionary after an initially unique phase', async () => {
    const distinct = Array.from({ length: 1000 }, (_, i) => String(i).padStart(8, '0').repeat(8))
    const data = [...distinct, ...new Array(4000).fill('repeated'.repeat(8))]
    /** @type {ColumnSource[]} */
    const columnData = [{ name: 'message', data, type: 'STRING' }]

    const buffer = parquetWriteBuffer({ columnData, rowGroupSize: data.length })

    const column = parquetMetadata(buffer).row_groups[0].columns[0]
    expect(column.meta_data?.encodings).toContain('RLE_DICTIONARY')

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toHaveLength(data.length)
    expect(rows[0].message).toBe(distinct[0])
    expect(rows[999].message).toBe(distinct[999])
    expect(rows[1000].message).toBe('repeated'.repeat(8))
  })

  it('uses indexed plain pages when a small dictionary does not halve bytes', async () => {
    const distinct = Array.from({ length: 400 }, (_, i) => String(i).padStart(8, '0').repeat(4))
    let distinctIndex = 0
    const data = Array.from({ length: 1000 }, (_, i) => {
      return i % 5 < 2 ? distinct[distinctIndex++] : 'repeated'
    })
    /** @type {ColumnSource[]} */
    const columnData = [{ name: 'message', data, type: 'STRING' }]

    const buffer = parquetWriteBuffer({ columnData, pageSize: 4096, rowGroupSize: data.length })

    const column = parquetMetadata(buffer).row_groups[0].columns[0]
    expect(column.meta_data?.encodings).toEqual(['PLAIN'])
    expect(column.offset_index_offset).toBeDefined()

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows).toHaveLength(data.length)
    expect(rows[0].message).toBe(distinct[0])
    expect(rows[2].message).toBe('repeated')
  })

  it('honors an explicit dictionarySize cap', () => {
    // same low-cardinality data, but the caller caps dictionary bytes below
    // the distinct-value total, forcing plain encoding
    const distinct = Array.from({ length: 40 }, (_, i) => String(i).padStart(6, '0').repeat(500))
    const data = Array.from({ length: 400 }, (_, i) => distinct[i % 40])
    /** @type {ColumnSource[]} */
    const columnData = [{ name: 'prompt', data, type: 'STRING' }]

    const buffer = parquetWriteBuffer({ columnData, dictionarySize: 1024 })

    const column = parquetMetadata(buffer).row_groups[0].columns[0]
    expect(column.meta_data?.encodings).not.toContain('RLE_DICTIONARY')
  })

  it('honors forced RLE_DICTIONARY on high-cardinality values', async () => {
    // every value unique: the sampling heuristic would fall back to PLAIN, but
    // then the pages would be mislabeled as dictionary-encoded and misread.
    // A forced encoding must produce genuinely dictionary-encoded pages.
    const numRows = 1500
    const data = Array.from({ length: numRows }, (_, i) => `value-${i}`)
    /** @type {ColumnSource[]} */
    const columnData = [{ name: 'id', data, type: 'STRING', encoding: 'RLE_DICTIONARY' }]

    const buffer = parquetWriteBuffer({ columnData })

    const column = parquetMetadata(buffer).row_groups[0].columns[0]
    expect(column.meta_data?.encodings).toContain('RLE_DICTIONARY')

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    for (let i = 0; i < numRows; i++) {
      expect(rows[i].id).toBe(`value-${i}`)
    }
  })
})
