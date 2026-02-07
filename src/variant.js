const encoder = new TextEncoder()
const INT64_MIN = -(2n ** 63n)
const INT64_MAX = 2n ** 63n - 1n

/**
 * Encode an array of arbitrary JS values into variant binary format.
 * Each row becomes { metadata, value } (or null for missing values).
 * When shredding is provided, produces { metadata, value, typed_value } per row.
 *
 * @import {BasicType} from '../src/types.js'
 * @param {any[]} values
 * @param {Record<string, BasicType>} [shredding]
 * @returns {Array<Record<string, any> | null>}
 */
export function encodeVariantColumn(values, shredding) {
  const dictionary = buildVariantDictionary(values)
  const metadata = writeVariantMetadata(dictionary)
  /** @type {Map<string, number>} */
  const keyIndex = new Map()
  for (let i = 0; i < dictionary.length; i++) {
    keyIndex.set(dictionary[i], i)
  }
  if (shredding) {
    const fieldNames = Object.keys(shredding)
    return values.map(value => {
      if (value === undefined) return null
      return encodeVariantRowShredded(value, metadata, keyIndex, shredding, fieldNames)
    })
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
 * @param {any} value
 * @param {Uint8Array} metadata
 * @param {Map<string, number>} keyIndex
 * @param {Record<string, BasicType>} shredding
 * @param {string[]} fieldNames
 * @returns {Record<string, any>}
 */
function encodeVariantRowShredded(value, metadata, keyIndex, shredding, fieldNames) {
  // null -> value: variant null, typed_value: null
  if (value === null) {
    return { metadata, value: new Uint8Array([0x00]), typed_value: null }
  }

  // non-object -> value: binary variant, typed_value: null
  if (typeof value !== 'object' || Array.isArray(value) || value instanceof Date || value instanceof Uint8Array) {
    return { metadata, value: writeVariantValue(value, keyIndex), typed_value: null }
  }

  // object -> split into shredded fields + remaining fields
  /** @type {Record<string, any>} */
  const typedValue = {}
  const shreddedKeys = new Set(fieldNames)

  for (const fieldName of fieldNames) {
    const fieldType = shredding[fieldName]
    if (!(fieldName in value)) {
      // missing field: both value and typed_value null
      typedValue[fieldName] = { value: null, typed_value: null }
    } else if (value[fieldName] === null || value[fieldName] === undefined) {
      // null field: value is variant null, typed_value null
      typedValue[fieldName] = { value: new Uint8Array([0x00]), typed_value: null }
    } else if (matchesType(value[fieldName], fieldType)) {
      // type matches: typed_value gets native value, value null
      typedValue[fieldName] = { value: null, typed_value: value[fieldName] }
    } else {
      // type mismatch: value gets binary variant, typed_value null
      typedValue[fieldName] = { value: writeVariantValue(value[fieldName], keyIndex), typed_value: null }
    }
  }

  // remaining (non-shredded) fields go into binary value
  const remainingKeys = Object.keys(value).filter(k => !shreddedKeys.has(k))
  /** @type {Uint8Array | null} */
  let binaryValue = null
  if (remainingKeys.length > 0) {
    /** @type {Record<string, any>} */
    const remaining = {}
    for (const k of remainingKeys) {
      remaining[k] = value[k]
    }
    binaryValue = writeVariantValue(remaining, keyIndex)
  }

  return { metadata, value: binaryValue, typed_value: typedValue }
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

  return Object.keys(shredding).length > 0 ? shredding : undefined
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
  // Encode all strings first to compute offsets
  const encoded = dictionary.map(s => encoder.encode(s))
  const totalStringBytes = encoded.reduce((sum, e) => sum + e.length, 0)

  // Determine offset size: max offset is totalStringBytes
  const offsetSize = byteWidth(totalStringBytes)

  // Header: version=1, sorted=1, offsetSize
  const header = 1 | 1 << 4 | offsetSize - 1 << 6

  // Total size: 1 (header) + offsetSize (dict size) + (dict.length + 1) * offsetSize (offsets) + totalStringBytes
  const totalSize = 1 + offsetSize + (dictionary.length + 1) * offsetSize + totalStringBytes
  const buffer = new ArrayBuffer(totalSize)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)
  let offset = 0

  // Write header
  view.setUint8(offset++, header)

  // Write dictionary size
  writeUnsigned(view, offset, dictionary.length, offsetSize)
  offset += offsetSize

  // Write string offsets
  let strOffset = 0
  for (let i = 0; i < dictionary.length; i++) {
    writeUnsigned(view, offset, strOffset, offsetSize)
    offset += offsetSize
    strOffset += encoded[i].length
  }
  // Final offset
  writeUnsigned(view, offset, strOffset, offsetSize)
  offset += offsetSize

  // Write string data
  for (const enc of encoded) {
    bytes.set(enc, offset)
    offset += enc.length
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
  /** @type {number[]} */
  const parts = []
  writeValue(value, parts)
  return new Uint8Array(parts)

  /**
   * @param {any} val
   * @param {number[]} out
   */
  function writeValue(val, out) {
    // null
    if (val === null || val === undefined) {
      out.push(0x00) // basicType=0, typeId=0
      return
    }

    // boolean
    if (val === true) {
      out.push(0x04) // basicType=0, typeId=1 (1 << 2 | 0)
      return
    }
    if (val === false) {
      out.push(0x08) // basicType=0, typeId=2 (2 << 2 | 0)
      return
    }

    // bigint
    if (typeof val === 'bigint') {
      if (val < INT64_MIN || val > INT64_MAX) {
        throw new RangeError(`variant bigint out of int64 range: ${val}`)
      }
      out.push(6 << 2 | 0) // basicType=0, typeId=6
      writeLittleEndian64(val, out)
      return
    }

    // number
    if (typeof val === 'number') {
      if (Number.isInteger(val)) {
        if (val >= -128 && val <= 127) {
          out.push(3 << 2 | 0) // int8
          out.push(val & 0xff)
          return
        }
        if (val >= -32768 && val <= 32767) {
          out.push(4 << 2 | 0) // int16
          writeLittleEndian16(val, out)
          return
        }
        if (val >= -2147483648 && val <= 2147483647) {
          out.push(5 << 2 | 0) // int32
          writeLittleEndian32(val, out)
          return
        }
      }
      // double
      out.push(7 << 2 | 0) // typeId=7
      writeLittleEndianF64(val, out)
      return
    }

    // string
    if (typeof val === 'string') {
      const strBytes = encoder.encode(val)
      if (strBytes.length <= 63) {
        // short string: basicType=1, length in header
        out.push(strBytes.length << 2 | 1)
        for (const b of strBytes) out.push(b)
      } else {
        // long string: primitive typeId=16
        out.push(16 << 2 | 0)
        writeLittleEndianU32(strBytes.length, out)
        for (const b of strBytes) out.push(b)
      }
      return
    }

    // Date to timestamp_micros_ntz (typeId=13)
    if (val instanceof Date) {
      out.push(13 << 2 | 0) // basicType=0, typeId=13
      const micros = BigInt(val.getTime()) * 1000n
      writeLittleEndian64(micros, out)
      return
    }

    // Uint8Array to binary (typeId=15)
    if (val instanceof Uint8Array) {
      out.push(15 << 2 | 0) // basicType=0, typeId=15
      writeLittleEndianU32(val.length, out)
      for (const b of val) out.push(b)
      return
    }

    // Array
    if (Array.isArray(val)) {
      writeVariantArray(val, out)
      return
    }

    // Object
    if (typeof val === 'object') {
      writeVariantObject(val, out)
      return
    }

    throw new Error(`variant cannot encode value: ${val}`)
  }

  /**
   * @param {Record<string, any>} obj
   * @param {number[]} out
   */
  function writeVariantObject(obj, out) {
    const entries = Object.keys(obj).map(key => {
      const id = keyIndex.get(key)
      if (id === undefined) throw new Error(`variant key not in dictionary: ${key}`)
      return { id, key }
    })
    // Sort by field ID for spec compliance
    entries.sort((a, b) => a.id - b.id)

    const numElements = entries.length
    const maxFieldId = numElements > 0 ? entries[numElements - 1].id : 0
    const idWidth = byteWidth(maxFieldId)

    // Encode values to compute offsets
    /** @type {number[][]} */
    const valueParts = entries.map(({ key }) => {
      /** @type {number[]} */
      const vp = []
      writeValue(obj[key], vp)
      return vp
    })

    // Compute offsets
    const offsets = new Array(numElements + 1)
    offsets[0] = 0
    for (let i = 0; i < numElements; i++) {
      offsets[i + 1] = offsets[i] + valueParts[i].length
    }
    const maxOffset = offsets[numElements]
    const offsetWidth = byteWidth(maxOffset)

    const isLarge = numElements > 255 ? 1 : 0

    // Header: basicType=2, header encodes offsetWidth, idWidth, isLarge
    const header = (offsetWidth - 1 | idWidth - 1 << 2 | isLarge << 4) << 2 | 2
    out.push(header)

    // numElements
    if (isLarge) {
      writeLittleEndianU32(numElements, out)
    } else {
      out.push(numElements)
    }

    // Field IDs
    for (const { id } of entries) {
      writeUnsignedToArray(id, idWidth, out)
    }

    // Offsets
    for (const off of offsets) {
      writeUnsignedToArray(off, offsetWidth, out)
    }

    // Values
    for (const vp of valueParts) {
      for (const b of vp) out.push(b)
    }
  }

  /**
   * @param {any[]} arr
   * @param {number[]} out
   */
  function writeVariantArray(arr, out) {
    const numElements = arr.length

    // Encode elements to compute offsets
    /** @type {number[][]} */
    const elementParts = arr.map(item => {
      /** @type {number[]} */
      const ep = []
      writeValue(item, ep)
      return ep
    })

    const offsets = new Array(numElements + 1)
    offsets[0] = 0
    for (let i = 0; i < numElements; i++) {
      offsets[i + 1] = offsets[i] + elementParts[i].length
    }
    const maxOffset = offsets[numElements]
    const offsetWidth = byteWidth(maxOffset)

    const isLarge = numElements > 255 ? 1 : 0

    // Header: basicType=3, header encodes fieldOffsetSize, isLarge
    const header = (offsetWidth - 1 | isLarge << 2) << 2 | 3
    out.push(header)

    // numElements
    if (isLarge) {
      writeLittleEndianU32(numElements, out)
    } else {
      out.push(numElements)
    }

    // Offsets
    for (const off of offsets) {
      writeUnsignedToArray(off, offsetWidth, out)
    }

    // Values
    for (const ep of elementParts) {
      for (const b of ep) out.push(b)
    }
  }
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
 * Write an unsigned integer in little-endian format into a DataView.
 *
 * @param {DataView} view
 * @param {number} offset
 * @param {number} value
 * @param {number} width byte width (1-4)
 */
function writeUnsigned(view, offset, value, width) {
  for (let i = 0; i < width; i++) {
    view.setUint8(offset + i, value >> i * 8 & 0xff)
  }
}

/**
 * Write an unsigned integer in little-endian format into a number array.
 *
 * @param {number} value
 * @param {number} width byte width (1-4)
 * @param {number[]} out
 */
function writeUnsignedToArray(value, width, out) {
  for (let i = 0; i < width; i++) {
    out.push(value >> i * 8 & 0xff)
  }
}

/**
 * @param {number} value
 * @param {number[]} out
 */
function writeLittleEndian16(value, out) {
  out.push(value & 0xff)
  out.push(value >> 8 & 0xff)
}

/**
 * @param {number} value
 * @param {number[]} out
 */
function writeLittleEndian32(value, out) {
  out.push(value & 0xff)
  out.push(value >> 8 & 0xff)
  out.push(value >> 16 & 0xff)
  out.push(value >> 24 & 0xff)
}

/**
 * @param {number} value
 * @param {number[]} out
 */
function writeLittleEndianU32(value, out) {
  out.push(value & 0xff)
  out.push(value >>> 8 & 0xff)
  out.push(value >>> 16 & 0xff)
  out.push(value >>> 24 & 0xff)
}

/**
 * @param {bigint} value
 * @param {number[]} out
 */
function writeLittleEndian64(value, out) {
  const buf = new ArrayBuffer(8)
  new DataView(buf).setBigInt64(0, value, true)
  const bytes = new Uint8Array(buf)
  for (const b of bytes) out.push(b)
}

/**
 * @param {number} value
 * @param {number[]} out
 */
function writeLittleEndianF64(value, out) {
  const buf = new ArrayBuffer(8)
  new DataView(buf).setFloat64(0, value, true)
  const bytes = new Uint8Array(buf)
  for (const b of bytes) out.push(b)
}
