import { BloomBuilder } from './bloom.js'
import { writeDataPageV2 } from './datapage.js'
import { estimateValueSize, useDictionary, writeDictionaryPage } from './dictionary.js'
import { geospatialStatistics } from './geospatial.js'
import { unconvert, unconvertMinMax } from './unconvert.js'

/**
 * @import {ColumnChunk, ColumnIndex, DecodedArray, Encoding, OffsetIndex, ParquetType, Statistics} from 'hyparquet'
 * @import {PageEncodingStats} from 'hyparquet/src/types.js'
 * @import {ColumnEncoder, PageData, Writer} from '../src/types.js'
 */

/**
 * Write a column chunk to the writer.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {ColumnEncoder} options.column
 * @param {PageData} options.pageData
 * @returns {{ chunk: ColumnChunk, columnIndex?: ColumnIndex, offsetIndex?: OffsetIndex, bloomFilter?: Uint32Array }}
 */
export function writeColumn({ writer, column, pageData }) {
  const { columnName, element, schemaPath, stats, pageSize, encoding: userEncoding } = column
  const { type, type_length } = element
  if (!type) throw new Error(`column ${columnName} cannot determine type`)
  const { values, definitionLevels, repetitionLevels, maxDefinitionLevel } = pageData
  const offsetStart = writer.offset

  /** @type {Encoding[]} */
  const encodings = []

  const isGeospatial = element?.logical_type?.type === 'GEOMETRY' || element?.logical_type?.type === 'GEOGRAPHY'

  // Compute statistics
  const statistics = stats ? getStatistics(values) : undefined
  const geospatial_statistics = stats && isGeospatial ? geospatialStatistics(values) : undefined

  // Build bloom filter from original values (hashParquetValue reads schema info from element)
  let bloomFilter
  if (column.bloomFilter) {
    const opts = typeof column.bloomFilter === 'object' ? column.bloomFilter : undefined
    const builder = new BloomBuilder(element, opts)
    for (const v of values) builder.insert(v)
    bloomFilter = builder.finalize()
  }

  // dictionary encoding
  /** @type {bigint | undefined} */
  let dictionary_page_offset
  const { dictionary, indexes } = useDictionary(values, type, type_length, userEncoding, pageSize)

  // Determine encoding and prepare values for writing
  /** @type {Encoding} */
  let encoding
  /** @type {DecodedArray} */
  let writeValues
  let writeType = type
  if (dictionary && indexes) {
    // replace values with dictionary indices
    writeValues = indexes
    writeType = 'INT32'
    encoding = 'RLE_DICTIONARY'

    // write dictionary page first
    dictionary_page_offset = BigInt(writer.offset)
    const unconverted = unconvert(element, dictionary)
    writeDictionaryPage(writer, column, unconverted)
  } else {
    // unconvert values from rich types to simple
    writeValues = unconvert(element, values)
    encoding = userEncoding ?? (type === 'BOOLEAN' && values.length > 16 ? 'RLE' : 'PLAIN')
  }
  encodings.push(encoding)

  // Split values into pages based on pageSize
  const pageBoundaries = getPageBoundaries(writeValues, writeType, type_length, pageSize)

  // Initialize index structures if requested
  /** @type {ColumnIndex | undefined} */
  const columnIndex = column.columnIndex && pageBoundaries.length > 1 ? {
    null_pages: [],
    min_values: [],
    max_values: [],
    boundary_order: 'UNORDERED',
    null_counts: [],
  } : undefined
  /** @type {OffsetIndex | undefined} */
  const offsetIndex = column.offsetIndex && pageBoundaries.length > 1 ? {
    page_locations: [],
  } : undefined

  // Write data pages
  const data_page_offset = BigInt(writer.offset)
  let first_row_index = 0n
  let prevStart = 0
  let prevMinValue
  let prevMaxValue
  let ascending = true
  let descending = true

  for (const { start, end } of pageBoundaries) {
    const pageOffset = writer.offset

    // Slice into subpage and write levels and data
    const pageChunk = {
      values: writeValues.slice(start, end),
      definitionLevels: definitionLevels.slice(start, end),
      repetitionLevels: repetitionLevels.slice(start, end),
      maxDefinitionLevel,
    }
    writeDataPageV2({ writer, column, encoding, pageData: pageChunk })

    // ColumnIndex construction
    if (columnIndex) {
      const pageValues = values.slice(start, end) // original values not indexes
      const { min_value, max_value, null_count = 0n } = getStatistics(pageValues)

      columnIndex.null_pages.push(null_count === BigInt(end - start)) // all nulls
      // Spec: for all-null pages set "byte[0]"
      columnIndex.min_values.push(unconvertMinMax(min_value, element) ?? new Uint8Array())
      columnIndex.max_values.push(unconvertMinMax(max_value, element) ?? new Uint8Array())
      columnIndex.null_counts?.push(null_count)

      // Track boundary order using original JS values
      if (prevMinValue !== undefined && min_value !== undefined) {
        if (prevMinValue > min_value) ascending = false
        if (prevMinValue < min_value) descending = false
      }
      if (prevMaxValue !== undefined && max_value !== undefined) {
        if (prevMaxValue > max_value) ascending = false
        if (prevMaxValue < max_value) descending = false
      }
      prevMinValue = min_value
      prevMaxValue = max_value
    }

    // OffsetIndex construction
    if (offsetIndex) {
      if (repetitionLevels.length) {
        // Count row boundaries from previous page
        for (let i = prevStart + 1; i <= start; i++) {
          if (repetitionLevels[i] === 0) first_row_index++
        }
      } else {
        first_row_index = BigInt(start) // Flat column
      }

      offsetIndex.page_locations.push({
        offset: BigInt(pageOffset),
        compressed_page_size: writer.offset - pageOffset,
        first_row_index,
      })
    }

    prevStart = start
  }

  // Set boundary order after all pages are written
  if (columnIndex) {
    if (ascending) columnIndex.boundary_order = 'ASCENDING'
    else if (descending) columnIndex.boundary_order = 'DESCENDING'
  }

  // Build encoding stats
  /** @type {PageEncodingStats[] | undefined} */
  let encoding_stats
  if (stats) {
    encoding_stats = []
    if (dictionary_page_offset !== undefined) {
      encoding_stats.push({ page_type: 'DICTIONARY_PAGE', encoding: 'PLAIN', count: 1 })
    }
    encoding_stats.push({ page_type: 'DATA_PAGE_V2', encoding, count: pageBoundaries.length })
  }

  return {
    chunk: {
      meta_data: {
        type,
        encodings,
        path_in_schema: schemaPath.slice(1).map(s => s.name),
        codec: column.codec ?? 'UNCOMPRESSED',
        num_values: BigInt(values.length),
        total_compressed_size: BigInt(writer.offset - offsetStart),
        total_uncompressed_size: BigInt(writer.offset - offsetStart), // TODO: uncompressed pages + headers
        data_page_offset,
        dictionary_page_offset,
        statistics,
        encoding_stats,
        geospatial_statistics,
      },
      file_offset: BigInt(offsetStart),
    },
    columnIndex,
    offsetIndex,
    bloomFilter,
  }
}

/**
 * Get page boundaries based on estimated byte size.
 * TODO: split pages on row boundaries
 *
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number | undefined} type_length
 * @param {number} pageSize
 * @returns {{start: number, end: number}[]}
 */
function getPageBoundaries(values, type, type_length, pageSize) {
  // If no pageSize limit, return single page with all values
  if (!pageSize) {
    return [{ start: 0, end: values.length }]
  }

  const boundaries = []
  let start = 0
  let accumulatedBytes = 0

  for (let i = 0; i < values.length; i++) {
    const valueSize = estimateValueSize(values[i], type, type_length)
    accumulatedBytes += valueSize

    // Check if we should start a new page
    if (accumulatedBytes >= pageSize && i > start) {
      boundaries.push({ start, end: i })
      start = i
      accumulatedBytes = valueSize
    }
  }

  // Final page with remaining values
  if (start < values.length) {
    boundaries.push({ start, end: values.length })
  }

  return boundaries
}

/**
 * @param {DecodedArray} values
 * @returns {Statistics}
 */
function getStatistics(values) {
  let min_value = undefined
  let max_value = undefined
  let null_count = 0n
  for (const value of values) {
    if (value === null || value === undefined) {
      null_count++
      continue
    }
    if (typeof value === 'object') continue // skip objects
    if (typeof value === 'number' && Number.isNaN(value)) continue // skip NaN per parquet spec
    if (min_value === undefined || value < min_value) min_value = value
    if (max_value === undefined || value > max_value) max_value = value
  }
  // Normalize signed zero per parquet spec: min becomes -0, max becomes +0
  if (min_value === 0) min_value = -0
  if (max_value === 0) max_value = 0
  return { min_value, max_value, null_count }
}
