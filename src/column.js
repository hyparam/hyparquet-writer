import { ByteWriter } from './bytewriter.js'
import { writeDataPageV2, writePageHeader } from './datapage.js'
import { geospatialStatistics } from './geospatial.js'
import { writePlain } from './plain.js'
import { unconvert, unconvertMinMax } from './unconvert.js'

/**
 * @import {ColumnChunk, ColumnIndex, DecodedArray, Encoding, OffsetIndex, ParquetType, Statistics} from 'hyparquet'
 * @import {ColumnEncoder, PageData, Writer} from '../src/types.js'
 */

/**
 * Write a column chunk to the writer.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {ColumnEncoder} options.column
 * @param {PageData} options.pageData
 * @returns {{ chunk: ColumnChunk, columnIndex?: ColumnIndex, offsetIndex?: OffsetIndex }}
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

  // dictionary encoding
  /** @type {bigint | undefined} */
  let dictionary_page_offset
  const { dictionary, indexes } = useDictionary(values, type, type_length, userEncoding, pageSize)

  // Determine encoding and prepare values for writing
  /** @type {Encoding} */
  let encoding
  /** @type {DecodedArray} */
  let writeValues
  if (dictionary && indexes) {
    // replace values with dictionary indices
    writeValues = indexes
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
  const pageBoundaries = getPageBoundaries(writeValues, type, type_length, pageSize)

  // Initialize index structures if requested
  /** @type {ColumnIndex | undefined} */
  const columnIndex = column.columnIndex ? {
    null_pages: [],
    min_values: [],
    max_values: [],
    boundary_order: 'UNORDERED',
    null_counts: [],
  } : undefined
  /** @type {OffsetIndex | undefined} */
  const offsetIndex = column.offsetIndex ? {
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
      const originalSlice = values.slice(start, end)
      const pageStats = getStatistics(originalSlice)
      const nullCount = pageStats.null_count ?? 0n

      columnIndex.null_pages.push(nullCount === BigInt(end - start)) // all nulls
      // Spec: for all-null pages set "byte[0]"
      columnIndex.min_values.push(unconvertMinMax(pageStats.min_value, element) ?? new Uint8Array())
      columnIndex.max_values.push(unconvertMinMax(pageStats.max_value, element) ?? new Uint8Array())
      columnIndex.null_counts?.push(nullCount)

      // Track boundary order using original JS values
      if (prevMinValue !== undefined && pageStats.min_value !== undefined) {
        if (prevMinValue > pageStats.min_value) ascending = false
        if (prevMinValue < pageStats.min_value) descending = false
      }
      if (prevMaxValue !== undefined && pageStats.max_value !== undefined) {
        if (prevMaxValue > pageStats.max_value) ascending = false
        if (prevMaxValue < pageStats.max_value) descending = false
      }
      prevMinValue = pageStats.min_value
      prevMaxValue = pageStats.max_value
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
    const numPages = columnIndex.min_values.length
    columnIndex.boundary_order = numPages < 2 ? 'UNORDERED'
      : ascending ? 'ASCENDING' : descending ? 'DESCENDING' : 'UNORDERED'
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
        geospatial_statistics,
      },
      file_offset: BigInt(offsetStart),
    },
    columnIndex,
    offsetIndex,
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
 * Estimate the byte size of a value for page size calculation.
 *
 * @param {any} value
 * @param {ParquetType} type
 * @param {number} [type_length]
 * @returns {number}
 */
function estimateValueSize(value, type, type_length) {
  if (value === null || value === undefined) return 0
  if (type === 'BOOLEAN') return 1 // bit, but count as byte for simplicity
  if (type === 'INT32' || type === 'FLOAT') return 4
  if (type === 'INT64' || type === 'DOUBLE') return 8
  if (type === 'INT96') return 12
  if (type === 'FIXED_LEN_BYTE_ARRAY') return type_length ?? 0
  if (type === 'BYTE_ARRAY') {
    if (value instanceof Uint8Array) return value.byteLength
    if (typeof value === 'string') return value.length
  }
  return 0
}

/**
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number | undefined} type_length
 * @param {Encoding | undefined} encoding
 * @param {number} pageSize
 * @returns {{ dictionary?: any[], indexes?: number[] }}
 */
function useDictionary(values, type, type_length, encoding, pageSize) {
  if (encoding && encoding !== 'RLE_DICTIONARY') return {}
  if (type === 'BOOLEAN') return {}

  // uniqueness on a sample
  const sample = values.slice(0, 1000)
  const sampleUnique = new Set(sample).size
  if (sampleUnique === 0 || sampleUnique / sample.length > 0.5) return {}

  // build dictionary and indexes
  /** @type {Map<any, number>} */
  const unique = new Map()
  /** @type {number[]} */
  const indexes = new Array(values.length)
  let dictSize = 0
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (value === null || value === undefined) continue

    // dictionary cannot exceed page size
    dictSize += estimateValueSize(value, type, type_length)
    if (pageSize && dictSize > pageSize) return {}

    // find index for value in dictionary
    let index = unique.get(value)
    if (index === undefined) {
      index = unique.size
      unique.set(value, index)
    }
    indexes[i] = index
  }

  // TODO: sort by frequency?
  return { dictionary: Array.from(unique.keys()), indexes }
}

/**
 * @param {Writer} writer
 * @param {ColumnEncoder} column
 * @param {DecodedArray} dictionary
 */
function writeDictionaryPage(writer, column, dictionary) {
  const { element, codec, compressors } = column
  const { type, type_length } = element
  if (!type) throw new Error(`column ${column.columnName} cannot determine type`)

  // write values to temp buffer
  const dictionaryPage = new ByteWriter()
  writePlain(dictionaryPage, dictionary, type, type_length)
  const dictionaryBytes = dictionaryPage.getBytes()

  // compress dictionary page data
  const compressedBytes = compressors[codec]?.(dictionaryBytes) ?? dictionaryBytes

  // write dictionary page header
  writePageHeader(writer, {
    type: 'DICTIONARY_PAGE',
    uncompressed_page_size: dictionaryBytes.byteLength,
    compressed_page_size: compressedBytes.byteLength,
    dictionary_page_header: {
      num_values: dictionary.length,
      encoding: 'PLAIN',
    },
  })
  writer.appendBytes(compressedBytes)
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
    if (min_value === undefined || value < min_value) min_value = value
    if (max_value === undefined || value > max_value) max_value = value
  }
  return { min_value, max_value, null_count }
}
