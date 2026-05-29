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
 * @import {BasicType, ShredType} from '../src/types.js'
 * @param {any[]} values
 * @param {ShredType | undefined} shredding
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
      // undefined is a missing row; null is a present Variant null.
      if (value === undefined) return null
      // Build the metadata dictionary from every nested key in the row. The
      // reader uses dictionary membership to decide which object fields are
      // present, so all present keys (shredded or not) must be in the dictionary.
      /** @type {Set<string>} */
      const keys = new Set()
      collectKeys(value, keys)
      const { metadata, keyIndex } = getVariantRowMetadata(keys, metadataCache)
      return { metadata, ...encodeShredded(value, shreddingConfig, keyIndex, true) }
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
 * Recursively encode a value against a shred type into a { value, typed_value }
 * shredded group (the metadata wrapper is added by the caller at the top level).
 *
 * Shape rules (per the Variant shredding spec):
 * - scalar: matches the type -> typed_value holds the value, value is null;
 *   otherwise fall back to a binary variant in value.
 * - object: shredded fields go into the typed_value struct (absent fields are
 *   omitted), remaining fields are packed into a binary value.
 * - array: each element is recursively shredded into the typed_value LIST, value
 *   is null. A non-array value falls back to a binary value.
 *
 * @param {any} value
 * @param {ShredType} shredType
 * @param {Map<string, number>} keyIndex
 * @param {boolean} allowPartialObjects
 * @returns {{ value: Uint8Array | null, typed_value: any }}
 */
function encodeShredded(value, shredType, keyIndex, allowPartialObjects) {
  // Present Variant null: value holds variant null, typed_value is null.
  if (value === null || value === undefined) {
    return { value: VARIANT_NULL, typed_value: null }
  }

  // Array shred type
  if (Array.isArray(shredType)) {
    if (!Array.isArray(value)) {
      // Not an array: typed_value must be null, store the value as binary.
      return { value: writeVariantValue(value, keyIndex), typed_value: null }
    }
    const elemShred = shredType[0]
    return { value: null, typed_value: value.map(el => encodeShredded(el, elemShred, keyIndex, false)) }
  }

  // Object shred type
  if (typeof shredType === 'object') {
    // Not a plain object: fall back to a binary value.
    if (typeof value !== 'object' || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array) {
      return { value: writeVariantValue(value, keyIndex), typed_value: null }
    }

    // Remaining (non-shredded) fields are packed into a binary value.
    /** @type {Record<string, any>} */
    const remaining = {}
    let hasRemaining = false
    for (const k of Object.keys(value)) {
      if (k in shredType || value[k] === undefined) continue
      remaining[k] = value[k]
      hasRemaining = true
    }
    if (hasRemaining && !allowPartialObjects) {
      return { value: writeVariantValue(value, keyIndex), typed_value: null }
    }

    const fieldNames = Object.keys(shredType)
    const hasMissingFieldConflict = fieldNames.some(fieldName =>
      (!Object.prototype.hasOwnProperty.call(value, fieldName) || value[fieldName] === undefined) &&
      keyIndex.has(fieldName)
    )
    if (hasMissingFieldConflict) {
      return { value: writeVariantValue(value, keyIndex), typed_value: null }
    }

    /** @type {Record<string, any>} */
    const typedValue = {}
    for (const fieldName of fieldNames) {
      if (!Object.prototype.hasOwnProperty.call(value, fieldName) || value[fieldName] === undefined) {
        // missing field: omit the optional field wrapper entirely
        continue
      }
      typedValue[fieldName] = encodeShredded(value[fieldName], shredType[fieldName], keyIndex, false)
    }
    const binaryValue = hasRemaining ? writeVariantValue(remaining, keyIndex) : null

    return { value: binaryValue, typed_value: typedValue }
  }

  // Scalar shred type
  if (matchesType(value, shredType)) {
    return { value: null, typed_value: value }
  }
  return { value: writeVariantValue(value, keyIndex), typed_value: null }
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
 * Auto-detect a shredding config by recursively analyzing values for consistent
 * structure. Detects scalar fields, nested objects, and arrays. Only structured
 * top-level values (objects/arrays) are shredded; a column of bare scalars is
 * left unshredded.
 *
 * @param {any[]} values
 * @returns {ShredType | undefined}
 */
export function autoDetectShredding(values) {
  const detected = detectShred(values)
  // Top level: only shred structured values (objects/arrays), not bare scalars.
  if (detected === undefined || typeof detected !== 'object') return undefined
  return normalizeShreddingConfig(detected)
}

/**
 * Recursively detect a shred type from a pool of sample values at one position.
 * Returns undefined when the values are not consistently shreddable.
 *
 * @param {any[]} values
 * @returns {ShredType | undefined}
 */
function detectShred(values) {
  /** @type {any[]} */
  const nonNull = []
  for (const v of values) {
    if (v !== null && v !== undefined) nonNull.push(v)
  }
  if (!nonNull.length) return undefined

  // Object shred: any plain object present. Non-objects are ignored here and
  // fall back to binary at encode time.
  if (nonNull.some(isPlainObject)) {
    /** @type {Map<string, any[]>} field name -> its present values */
    const fieldValues = new Map()
    for (const v of nonNull) {
      if (!isPlainObject(v)) continue
      for (const [key, fieldValue] of Object.entries(v)) {
        if (fieldValue === undefined) continue
        const arr = fieldValues.get(key)
        if (arr) arr.push(fieldValue)
        else fieldValues.set(key, [fieldValue])
      }
    }
    /** @type {Record<string, ShredType>} */
    const shredding = {}
    for (const [key, vals] of fieldValues) {
      const fieldShred = detectShred(vals)
      if (fieldShred !== undefined) shredding[key] = fieldShred
    }
    return Object.keys(shredding).length > 0 ? shredding : undefined
  }

  // Array shred: every value is an array. Pool all elements and recurse.
  if (nonNull.every(Array.isArray)) {
    /** @type {any[]} */
    const elements = []
    for (const arr of nonNull) for (const el of arr) elements.push(el)
    const elemShred = detectShred(elements)
    return elemShred === undefined ? undefined : [elemShred]
  }

  // Scalar shred: every value is the same basic JS type.
  /** @type {string | undefined} */
  let jsType
  for (const v of nonNull) {
    if (Array.isArray(v)) return undefined // mixed array + scalar
    const t = v instanceof Date ? 'date' : typeof v
    if (jsType === undefined) jsType = t
    else if (jsType !== t) return undefined
  }
  return jsType ? jsTypeToBasicType(jsType) : undefined
}

/**
 * True for plain objects (not null, array, Date, or Uint8Array).
 *
 * @param {any} v
 * @returns {boolean}
 */
function isPlainObject(v) {
  return typeof v === 'object' && v !== null &&
    !Array.isArray(v) && !(v instanceof Date) && !(v instanceof Uint8Array)
}

/**
 * Recursively strip field names reserved by the shredded variant wrapper layout
 * (`value`, `typed_value`). Returns undefined when an object level empties out.
 *
 * @param {ShredType} shredding
 * @returns {ShredType | undefined}
 */
export function normalizeShreddingConfig(shredding) {
  if (Array.isArray(shredding)) {
    const elem = shredding.length ? normalizeShreddingConfig(shredding[0]) : undefined
    return elem === undefined ? undefined : [elem]
  }
  if (typeof shredding === 'object') {
    /** @type {Record<string, ShredType>} */
    const normalized = {}
    for (const [key, type] of Object.entries(shredding)) {
      if (RESERVED_SHREDDING_FIELDS.has(key)) continue
      const norm = normalizeShreddingConfig(type)
      if (norm !== undefined) normalized[key] = norm
    }
    return Object.keys(normalized).length > 0 ? normalized : undefined
  }
  // scalar
  return shredding
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
  return writer.getBytes()
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
