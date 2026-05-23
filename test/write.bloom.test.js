import { parquetMetadata } from 'hyparquet'
import { hashParquetValue, readBloomFilter } from 'hyparquet/src/bloom.js'
import { describe, expect, it } from 'vitest'
import { sbbfContains } from '../src/bloom.js'
import { ByteWriter } from '../src/bytewriter.js'
import { writeColumn } from '../src/column.js'
import { parquetWriteBuffer } from '../src/index.js'
import { snappyCompress } from '../src/snappy.js'

/**
 * @import {SchemaElement} from 'hyparquet'
 * @import {ColumnEncoder, PageData} from '../src/types.js'
 */

/**
 * @param {SchemaElement} element
 * @param {Partial<ColumnEncoder>} overrides
 * @returns {ColumnEncoder}
 */
function makeColumn(element, overrides = {}) {
  return {
    columnName: element.name,
    element,
    schemaPath: [{ name: 'root', num_children: 1 }, element],
    codec: 'UNCOMPRESSED',
    compressors: { SNAPPY: snappyCompress },
    stats: true,
    pageSize: 0,
    columnIndex: false,
    offsetIndex: false,
    ...overrides,
  }
}

/**
 * @param {any[]} values
 * @returns {PageData}
 */
function makePageData(values) {
  return {
    values,
    definitionLevels: [],
    repetitionLevels: [],
    maxDefinitionLevel: 0,
  }
}

describe('writeColumn bloom filter', () => {
  it('does not return a bloom filter when the flag is off', () => {
    const writer = new ByteWriter()
    const result = writeColumn({
      writer,
      column: makeColumn({ name: 'v', type: 'INT32', repetition_type: 'REQUIRED' }),
      pageData: makePageData([1, 2, 3]),
    })
    expect(result.bloomFilter).toBeUndefined()
  })

  it('returns a bloom filter containing each inserted value when enabled', () => {
    const writer = new ByteWriter()
    const values = ['alpha', 'beta', 'gamma']
    /** @type {SchemaElement} */
    const element = { name: 'v', type: 'BYTE_ARRAY', repetition_type: 'REQUIRED' }
    const result = writeColumn({
      writer,
      column: makeColumn(element, { bloomFilter: true }),
      pageData: makePageData(values),
    })
    expect(result.bloomFilter).toBeInstanceOf(Uint32Array)
    for (const v of values) {
      const h = hashParquetValue(v, element) ?? 0n
      expect(sbbfContains(result.bloomFilter ?? new Uint32Array(0), h)).toBe(true)
    }
  })

  it('respects fpp option', () => {
    const writer = new ByteWriter()
    /** @type {SchemaElement} */
    const element = { name: 'v', type: 'INT32', repetition_type: 'REQUIRED' }
    const values = Array.from({ length: 100 }, (_, i) => i)
    const tight = writeColumn({
      writer: new ByteWriter(),
      column: makeColumn(element, { bloomFilter: { fpp: 0.0001 } }),
      pageData: makePageData(values),
    }).bloomFilter
    const loose = writeColumn({
      writer,
      column: makeColumn(element, { bloomFilter: { fpp: 0.1 } }),
      pageData: makePageData(values),
    }).bloomFilter
    expect(tight?.byteLength).toBeGreaterThan(loose?.byteLength ?? 0)
  })

  it('returns undefined when the column type is unhashable (e.g. DATE)', () => {
    const writer = new ByteWriter()
    /** @type {SchemaElement} */
    const element = { name: 'd', type: 'INT32', converted_type: 'DATE', repetition_type: 'REQUIRED' }
    const result = writeColumn({
      writer,
      column: makeColumn(element, { bloomFilter: true }),
      pageData: makePageData([1, 2, 3]),
    })
    expect(result.bloomFilter).toBeUndefined()
  })
})

describe('parquetWriteBuffer bloom filter end-to-end', () => {
  it('writes bloom_filter_offset/length and round-trips via readBloomFilter', () => {
    const values = Array.from({ length: 500 }, (_, i) => `value-${i}`)
    const buffer = parquetWriteBuffer({
      columnData: [
        { name: 'plain', data: values.slice(), type: 'STRING' },
        { name: 'bloomed', data: values.slice(), type: 'STRING', bloomFilter: true },
      ],
    })
    const metadata = parquetMetadata(buffer)
    const [plain, bloomed] = metadata.row_groups[0].columns

    expect(plain.meta_data?.bloom_filter_offset).toBeUndefined()
    expect(bloomed.meta_data?.bloom_filter_offset).toBeDefined()
    expect(bloomed.meta_data?.bloom_filter_length).toBeGreaterThan(0)

    const offset = Number(bloomed.meta_data?.bloom_filter_offset)
    const length = bloomed.meta_data?.bloom_filter_length ?? 0
    const reader = { view: new DataView(buffer, offset, length), offset: 0 }
    const parsed = readBloomFilter(reader)
    expect(parsed).toBeDefined()

    /** @type {SchemaElement} */
    const element = { name: 'bloomed', type: 'BYTE_ARRAY' }
    for (const v of values) {
      const h = hashParquetValue(v, element) ?? 0n
      expect(sbbfContains(parsed?.blocks ?? new Uint32Array(0), h)).toBe(true)
    }
  })

  it('writes one bloom per row group when bloomFilter is enabled', () => {
    const values = Array.from({ length: 300 }, (_, i) => i)
    const buffer = parquetWriteBuffer({
      columnData: [{ name: 'v', data: values, type: 'INT32', bloomFilter: true }],
      rowGroupSize: 100,
    })
    const metadata = parquetMetadata(buffer)
    expect(metadata.row_groups).toHaveLength(3)
    for (const rg of metadata.row_groups) {
      expect(rg.columns[0].meta_data?.bloom_filter_offset).toBeDefined()
      expect(rg.columns[0].meta_data?.bloom_filter_length).toBeGreaterThan(0)
    }
  })
})
