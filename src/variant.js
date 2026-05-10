import { ByteWriter } from './bytewriter.js'

const encoder = new TextEncoder()
const INT64_MIN = -(2n ** 63n)
const INT64_MAX = 2n ** 63n - 1n
const VARIANT_NULL = new Uint8Array([0x00])
const RESERVED_SHREDDING_FIELDS = new Set(['value', 'typed_value'])
/** @type {Map<string, number>} */
const EMPTY_KEY_INDEX = new Map()
const EMPTY_METADATA = writeVariantMetadata([])

/**
 * Encode an array of arbitrary JS values into variant binary format.
 * Each row becomes { metadata, value } (or null for missing values).
 * When shredding is provided, produces { metadata, value, typed_value } per row.
 *
 * @import {BasicType} from '../src/types.js'
 * @param {any[]} values
 * @param {Record<string, BasicType> | undefined} shredding
 * @param {{ name: string, required: boolean }} [column]
 * @returns {Array<Record<string, any> | null>}
 */
export function encodeVariantColumn(values, shredding, column) {
  if (column?.required) {
    for (let i = 0; i < values.length; i++) {
      if (values[i] === undefined) {
        throw new Error(`required variant column ${column.name} has undefined value at index ${i}`)
      }
    }
  }
  const shreddingConfig = shredding && normalizeShreddingConfig(shredding)
  if (shreddingConfig) {
    // Cache (metadata, keyIndex) by sorted-dictionary signature so rows with
    // the same set of keys share a single Uint8Array + Map.
    /** @type {Map<string, { metadata: Uint8Array, keyIndex: Map<string, number> }>} */
    const metadataCache = new Map()
    return values.map(value => {
      if (value === undefined) return null
      return encodeVariantRowShredded(value, shreddingConfig, metadataCache)
    })
  }

  const dictionary = buildVariantDictionary(values)
  const metadata = writeVariantMetadata(dictionary)
  /** @type {Map<string, number>} */
  const keyIndex = new Map()
  for (let i = 0; i < dictionary.length; i++) {
    keyIndex.set(dictionary[i], i)
  }
  return values.map(value => {
    // Keep top-level null as a present Variant null (0x00). Only undefined is missing.
    if (value === undefined) return null
    return { metadata, value: writeVariantValue(value, keyIndex) }
  })
}

/**
 * Encode a single row with variant shredding.
 * Splits object fields into shredded typed_value columns and remaining binary value.
 *
 * The hyparquet reader uses the metadata dictionary to determine which shredded
 * fields are present (vs. absent), so every present key in the row must appear
 * in the dictionary. We still skip recursion into shredded-matched primitive
 * values, and skip the keyIndex Map when no binary encoding is needed.
 *
 * @param {any} value
 * @param {Record<string, BasicType>} shredding
 * @param {Map<string, { metadata: Uint8Array, keyIndex: Map<string, number> }>} metadataCache
 * @returns {Record<string, any>}
 */
function encodeVariantRowShredded(value, shredding, metadataCache) {
  // null -> value: variant null, typed_value: null
  if (value === null) {
    return { metadata: EMPTY_METADATA, value: VARIANT_NULL, typed_value: null }
  }

  // scalar -> value: binary variant, typed_value: null (no dictionary needed)
  if (typeof value !== 'object' || value instanceof Date || value instanceof Uint8Array) {
    return { metadata: EMPTY_METADATA, value: writeVariantValue(value, EMPTY_KEY_INDEX), typed_value: null }
  }

  if (Array.isArray(value)) {
    /** @type {Set<string>} */
    const keys = new Set()
    collectKeys(value, keys)
    const { metadata, keyIndex } = getVariantRowMetadata(keys, metadataCache)
    return { metadata, value: writeVariantValue(value, keyIndex), typed_value: null }
  }

  // Single pass: collect dictionary keys and partition fields.
  // Recurse into mismatched-shredded and remaining values only.
  // Matched shredded primitive values don't contribute keys to the dictionary.
  /** @type {Set<string>} */
  const keys = new Set()
  /** @type {string[]} */
  const remainingKeys = []
  /** @type {Set<string>} */
  const mismatchedShredded = new Set()
  for (const k of Object.keys(value)) {
    keys.add(k)
    if (k in shredding) {
      const v = value[k]
      if (v !== null && v !== undefined && !matchesType(v, shredding[k])) {
        mismatchedShredded.add(k)
        collectKeys(v, keys)
      }
    } else {
      remainingKeys.push(k)
      collectKeys(value[k], keys)
    }
  }

  const { metadata, keyIndex } = getVariantRowMetadata(keys, metadataCache)

  // Build typed_value
  /** @type {Record<string, any>} */
  const typedValue = {}
  for (const fieldName of Object.keys(shredding)) {
    if (!(fieldName in value)) {
      // missing field: omit the optional field wrapper entirely
      typedValue[fieldName] = undefined
    } else if (value[fieldName] === null || value[fieldName] === undefined) {
      typedValue[fieldName] = { value: VARIANT_NULL, typed_value: null }
    } else if (mismatchedShredded.has(fieldName)) {
      typedValue[fieldName] = { value: writeVariantValue(value[fieldName], keyIndex), typed_value: null }
    } else {
      typedValue[fieldName] = { value: null, typed_value: value[fieldName] }
    }
  }

  // Build binary value from remaining fields
  let binaryValue = null
  if (remainingKeys.length > 0) {
    /** @type {Record<string, any>} */
    const remaining = {}
    for (const k of remainingKeys) remaining[k] = value[k]
    binaryValue = writeVariantValue(remaining, keyIndex)
  }

  return { metadata, value: binaryValue, typed_value: typedValue }
}

/**
 * Build metadata and keyIndex, sharing across rows with the same dictionary.
 *
 * @param {Set<string>} keys
 * @param {Map<string, { metadata: Uint8Array, keyIndex: Map<string, number> }>} metadataCache
 * @returns {{ metadata: Uint8Array, keyIndex: Map<string, number> }}
 */
function getVariantRowMetadata(keys, metadataCache) {
  if (keys.size === 0) {
    return { metadata: EMPTY_METADATA, keyIndex: EMPTY_KEY_INDEX }
  }

  const dictionary = [...keys].sort()
  const cacheKey = dictionary.join('\0')
  const cached = metadataCache.get(cacheKey)
  if (cached) {
    return cached
  }

  const metadata = writeVariantMetadata(dictionary)
  const keyIndex = new Map()
  for (let i = 0; i < dictionary.length; i++) keyIndex.set(dictionary[i], i)
  const rowMetadata = { metadata, keyIndex }
  metadataCache.set(cacheKey, rowMetadata)
  return rowMetadata
}

/**
 * Check if a JS value matches a BasicType for shredding.
 *
 * @param {any} value
 * @param {BasicType} type
 * @returns {boolean}
 */
function matchesType(value, type) {
  if (value === null || value === undefined) return false
  switch (type) {
  case 'BOOLEAN': return typeof value === 'boolean'
  case 'INT32': return typeof value === 'number' && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647
  case 'INT64': return typeof value === 'bigint' && value >= INT64_MIN && value <= INT64_MAX
  case 'FLOAT': return typeof value === 'number'
  case 'DOUBLE': return typeof value === 'number'
  case 'STRING': return typeof value === 'string'
  case 'TIMESTAMP': return value instanceof Date
  default: return false
  }
}

/**
 * Auto-detect shredding config by analyzing values for consistent field types.
 * Scans all object values and finds fields where every non-null occurrence has the same type.
 *
 * @param {any[]} values
 * @returns {Record<string, BasicType> | undefined}
 */
export function autoDetectShredding(values) {
  /** @type {Record<string, string>} field name -> detected JS type */
  const fieldTypes = {}
  /** @type {Record<string, boolean>} field name -> has consistent type */
  const consistent = {}
  let hasObjects = false

  for (const value of values) {
    if (value === null || value === undefined) continue
    if (typeof value !== 'object' || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array) continue
    hasObjects = true
    for (const [key, fieldValue] of Object.entries(value)) {
      if (fieldValue === null || fieldValue === undefined) continue
      const jsType = fieldValue instanceof Date ? 'date' : typeof fieldValue
      if (!(key in fieldTypes)) {
        fieldTypes[key] = jsType
        consistent[key] = true
      } else if (fieldTypes[key] !== jsType) {
        consistent[key] = false
      }
    }
  }

  if (!hasObjects) return undefined

  /** @type {Record<string, BasicType>} */
  const shredding = {}
  for (const [key, jsType] of Object.entries(fieldTypes)) {
    if (!consistent[key]) continue
    const basicType = jsTypeToBasicType(jsType)
    if (basicType) shredding[key] = basicType
  }

  return normalizeShreddingConfig(shredding)
}

/**
 * Remove field names reserved by the shredded variant wrapper layout.
 *
 * @param {Record<string, BasicType>} shredding
 * @returns {Record<string, BasicType> | undefined}
 */
export function normalizeShreddingConfig(shredding) {
  /** @type {Record<string, BasicType>} */
  const normalized = {}
  for (const [key, type] of Object.entries(shredding)) {
    if (RESERVED_SHREDDING_FIELDS.has(key)) continue
    normalized[key] = type
  }
  return Object.keys(normalized).length > 0 ? normalized : undefined
}

/**
 * Map a JS typeof string to a BasicType for shredding.
 *
 * @param {string} jsType
 * @returns {BasicType | undefined}
 */
function jsTypeToBasicType(jsType) {
  switch (jsType) {
  case 'boolean': return 'BOOLEAN'
  case 'string': return 'STRING'
  case 'number': return 'DOUBLE'
  case 'bigint': return 'INT64'
  case 'date': return 'TIMESTAMP'
  default: return undefined
  }
}

/**
 * Recursively collect all unique object keys from the column values.
 * Returns a sorted string array.
 *
 * @param {any[]} values
 * @returns {string[]}
 */
function buildVariantDictionary(values) {
  /** @type {Set<string>} */
  const keys = new Set()
  collectKeys(values, keys)
  return [...keys].sort()
}

/**
 * @param {any} value
 * @param {Set<string>} keys
 */
function collectKeys(value, keys) {
  if (value === null || value === undefined) return
  if (Array.isArray(value)) {
    for (const item of value) {
      collectKeys(item, keys)
    }
    return
  }
  if (value instanceof Date || value instanceof Uint8Array) return
  if (typeof value === 'object') {
    for (const key of Object.keys(value)) {
      keys.add(key)
      collectKeys(value[key], keys)
    }
  }
}

/**
 * Encode variant metadata binary.
 * Format: header byte, dictionary size, offsets, UTF-8 string data.
 *
 * @param {string[]} dictionary sorted array of unique keys
 * @returns {Uint8Array}
 */
function writeVariantMetadata(dictionary) {
  // Encode strings and compute total byte length in one pass
  const n = dictionary.length
  /** @type {Uint8Array[]} */
  const encoded = new Array(n)
  let totalStringBytes = 0
  for (let i = 0; i < n; i++) {
    const e = encoder.encode(dictionary[i])
    encoded[i] = e
    totalStringBytes += e.length
  }

  // Determine offset size: max offset is totalStringBytes
  const offsetSize = byteWidth(totalStringBytes)

  // Header: version=1, sorted=1, offsetSize
  const header = 1 | 1 << 4 | offsetSize - 1 << 6

  // Total size: 1 (header) + offsetSize (dict size) + (n + 1) * offsetSize (offsets) + totalStringBytes
  const totalSize = 1 + offsetSize + (n + 1) * offsetSize + totalStringBytes
  const bytes = new Uint8Array(totalSize)
  let offset = 0

  bytes[offset++] = header

  // Dictionary size
  for (let j = 0; j < offsetSize; j++) bytes[offset++] = n >> j * 8 & 0xff

  // String offsets
  let strOffset = 0
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < offsetSize; j++) bytes[offset++] = strOffset >> j * 8 & 0xff
    strOffset += encoded[i].length
  }
  // Final offset
  for (let j = 0; j < offsetSize; j++) bytes[offset++] = strOffset >> j * 8 & 0xff

  // String data
  for (let i = 0; i < n; i++) {
    bytes.set(encoded[i], offset)
    offset += encoded[i].length
  }

  return bytes
}

/**
 * Encode a single JS value to variant binary format.
 *
 * @param {any} value
 * @param {Map<string, number>} keyIndex map from key string to dictionary index
 * @returns {Uint8Array}
 */
function writeVariantValue(value, keyIndex) {
  const writer = new ByteWriter(8)
  writeValue(value, writer, keyIndex)
  return new Uint8Array(writer.getBuffer())
}

/**
 * @param {any} val
 * @param {ByteWriter} writer
 * @param {Map<string, number>} keyIndex
 */
function writeValue(val, writer, keyIndex) {
  if (val === null || val === undefined) {
    writer.appendUint8(0x00) // basicType=0, typeId=0
    return
  }
  if (val === true) {
    writer.appendUint8(0x04) // typeId=1
    return
  }
  if (val === false) {
    writer.appendUint8(0x08) // typeId=2
    return
  }
  if (typeof val === 'bigint') {
    if (val < INT64_MIN || val > INT64_MAX) {
      throw new RangeError(`variant bigint out of int64 range: ${val}`)
    }
    writer.appendUint8(6 << 2) // int64
    writer.appendInt64(val)
    return
  }
  if (typeof val === 'number') {
    if (Number.isInteger(val)) {
      if (val >= -128 && val <= 127) {
        writer.appendUint8(3 << 2) // int8
        writer.appendUint8(val & 0xff)
        return
      }
      if (val >= -32768 && val <= 32767) {
        writer.appendUint8(4 << 2) // int16
        appendUnsignedLE(writer, val, 2)
        return
      }
      if (val >= -2147483648 && val <= 2147483647) {
        writer.appendUint8(5 << 2) // int32
        writer.appendInt32(val)
        return
      }
    }
    writer.appendUint8(7 << 2) // double
    writer.appendFloat64(val)
    return
  }
  if (typeof val === 'string') {
    const strBytes = encoder.encode(val)
    if (strBytes.length <= 63) {
      // short string: basicType=1, length in header
      writer.appendUint8(strBytes.length << 2 | 1)
      writer.appendBytes(strBytes)
    } else {
      // long string: primitive typeId=16
      writer.appendUint8(16 << 2)
      writer.appendUint32(strBytes.length)
      writer.appendBytes(strBytes)
    }
    return
  }
  if (val instanceof Date) {
    writer.appendUint8(13 << 2) // timestamp_micros_ntz
    writer.appendInt64(BigInt(val.getTime()) * 1000n)
    return
  }
  if (val instanceof Uint8Array) {
    writer.appendUint8(15 << 2) // binary
    writer.appendUint32(val.length)
    writer.appendBytes(val)
    return
  }
  if (Array.isArray(val)) {
    writeVariantArray(val, writer, keyIndex)
    return
  }
  if (typeof val === 'object') {
    writeVariantObject(val, writer, keyIndex)
    return
  }

  throw new Error(`variant cannot encode value: ${val}`)
}

/**
 * @param {Record<string, any>} obj
 * @param {ByteWriter} writer
 * @param {Map<string, number>} keyIndex
 */
function writeVariantObject(obj, writer, keyIndex) {
  const entries = Object.keys(obj).filter(key => obj[key] !== undefined).map(key => {
    const id = keyIndex.get(key)
    if (id === undefined) throw new Error(`variant key not in dictionary: ${key}`)
    return { id, key }
  })
  // Sort by field ID for spec compliance
  entries.sort((a, b) => a.id - b.id)

  const numElements = entries.length
  const maxFieldId = numElements > 0 ? entries[numElements - 1].id : 0
  const idWidth = byteWidth(maxFieldId)

  // Encode child values into a scratch writer so we can compute offsets
  const scratch = new ByteWriter(8)
  const offsets = new Array(numElements + 1)
  offsets[0] = 0
  for (let i = 0; i < numElements; i++) {
    writeValue(obj[entries[i].key], scratch, keyIndex)
    offsets[i + 1] = scratch.index
  }
  const offsetWidth = byteWidth(offsets[numElements])
  const isLarge = numElements > 255 ? 1 : 0

  // Header: basicType=2, header encodes offsetWidth, idWidth, isLarge
  writer.appendUint8((offsetWidth - 1 | idWidth - 1 << 2 | isLarge << 4) << 2 | 2)
  if (isLarge) writer.appendUint32(numElements)
  else writer.appendUint8(numElements)
  for (const { id } of entries) appendUnsignedLE(writer, id, idWidth)
  for (const off of offsets) appendUnsignedLE(writer, off, offsetWidth)
  writer.appendBytes(scratch.getBytes())
}

/**
 * @param {any[]} arr
 * @param {ByteWriter} writer
 * @param {Map<string, number>} keyIndex
 */
function writeVariantArray(arr, writer, keyIndex) {
  const numElements = arr.length

  const scratch = new ByteWriter(8)
  const offsets = new Array(numElements + 1)
  offsets[0] = 0
  for (let i = 0; i < numElements; i++) {
    writeValue(arr[i], scratch, keyIndex)
    offsets[i + 1] = scratch.index
  }
  const offsetWidth = byteWidth(offsets[numElements])
  const isLarge = numElements > 255 ? 1 : 0

  // Header: basicType=3, header encodes fieldOffsetSize, isLarge
  writer.appendUint8((offsetWidth - 1 | isLarge << 2) << 2 | 3)
  if (isLarge) writer.appendUint32(numElements)
  else writer.appendUint8(numElements)
  for (const off of offsets) appendUnsignedLE(writer, off, offsetWidth)
  writer.appendBytes(scratch.getBytes())
}

/**
 * Determine the minimum byte width needed to represent a value.
 *
 * @param {number} maxValue
 * @returns {number} 1, 2, 3, or 4
 */
function byteWidth(maxValue) {
  if (maxValue <= 0xff) return 1
  if (maxValue <= 0xffff) return 2
  if (maxValue <= 0xffffff) return 3
  return 4
}

/**
 * Write an unsigned integer in little-endian format into a ByteWriter.
 *
 * @param {ByteWriter} writer
 * @param {number} value
 * @param {number} width byte width (1-4)
 */
function appendUnsignedLE(writer, value, width) {
  for (let i = 0; i < width; i++) {
    writer.appendUint8(value >> i * 8 & 0xff)
  }
}
