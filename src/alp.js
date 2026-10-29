/**
 * ALP (Adaptive Lossless floating-Point) encoding encoder.
 * Supports FLOAT and DOUBLE types.
 *
 * ALP encodes floating-point values by converting them to integers using
 * decimal scaling, then applying frame of reference (FOR) encoding and
 * bit-packing. Values that cannot be losslessly converted are stored as exceptions.
 */

import { ByteWriter } from './bytewriter.js'

const LOG_VECTOR_SIZE = 10 // 1024 elements per vector
const VECTOR_SIZE = 1 << LOG_VECTOR_SIZE
const SAMPLE_SIZE = 256 // values sampled per vector
const PRESET_SAMPLE_SIZE = 32 // values sampled per vector when choosing presets
const PRESET_VECTORS = 8 // vectors sampled when choosing presets
const MAX_PRESETS = 5 // (exponent, factor) candidates kept per page

// Correctly-rounded powers of ten (must match the decoder exactly)
const POW10 = [
  1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
  1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
]
const POW10_NEG = [
  1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9,
  1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18,
]
// float32 versions for FLOAT (exponent range [0, 10])
const POW10F = POW10.slice(0, 11).map(Math.fround)
const POW10F_NEG = POW10_NEG.slice(0, 11).map(Math.fround)

const TWO_32 = 2 ** 32
const TWO_63 = 2 ** 63

/**
 * Write ALP encoded data.
 *
 * @import {DecodedArray, ParquetType} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {DecodedArray} values non-null values
 * @param {ParquetType} type FLOAT or DOUBLE
 */
export function writeALP(writer, values, type) {
  /** @type {Float32Array | Float64Array} */
  let typed
  if (type === 'FLOAT') {
    typed = values instanceof Float32Array ? values : Float32Array.from(values)
  } else if (type === 'DOUBLE') {
    typed = values instanceof Float64Array ? values : Float64Array.from(values)
  } else {
    throw new Error(`ALP encoding unsupported type: ${type}`)
  }

  // encode each vector separately, so the offset array can be written up front
  const numVectors = Math.ceil(typed.length / VECTOR_SIZE)
  const presets = choosePresets(typed, numVectors)
  const vectors = []
  for (let v = 0; v < numVectors; v++) {
    const vector = new ByteWriter()
    writeVector(vector, typed.subarray(v * VECTOR_SIZE, (v + 1) * VECTOR_SIZE), presets)
    vectors.push(vector.getBytes())
  }

  // header (7 bytes)
  writer.appendUint8(0) // compression_mode: ALP
  writer.appendUint8(0) // integer_encoding: FOR + bit-packing
  writer.appendUint8(LOG_VECTOR_SIZE)
  writer.appendInt32(typed.length)

  // offset array, relative to its own start
  let offset = numVectors * 4
  for (const vector of vectors) {
    writer.appendUint32(offset)
    offset += vector.length
  }

  for (const vector of vectors) {
    writer.appendBytes(vector)
  }
}

/**
 * Write one vector: AlpInfo, ForInfo, packed values, and exceptions.
 *
 * @param {Writer} writer
 * @param {Float32Array | Float64Array} values
 * @param {[number, number][]} presets
 */
function writeVector(writer, values, presets) {
  const isFloat = values instanceof Float32Array
  const [exponent, factor] = presets.length === 1 ? presets[0] : bestExponentFactor(values, presets, SAMPLE_SIZE)

  // decimal encode, collecting exceptions
  const encoded = new Float64Array(values.length)
  /** @type {number[]} */
  const exceptions = []
  /** @type {number | undefined} */
  let placeholder
  for (let i = 0; i < values.length; i++) {
    const enc = encode(values[i], exponent, factor, isFloat)
    if (enc === undefined) {
      exceptions.push(i)
    } else {
      encoded[i] = enc
      placeholder ??= enc
    }
  }
  // replace exceptions with the first non-exception value to keep FOR range tight
  for (const pos of exceptions) {
    encoded[pos] = placeholder ?? 0
  }

  // frame of reference (max - min may round up past 2^64, so cap the width)
  let min = Infinity
  let max = -Infinity
  for (const enc of encoded) {
    if (enc < min) min = enc
    if (enc > max) max = enc
  }
  const bitWidth = Math.min(bitLength(max - min), 64)

  // AlpInfo
  writer.appendUint8(exponent)
  writer.appendUint8(factor)
  writer.appendUint8(exceptions.length & 0xff)
  writer.appendUint8(exceptions.length >> 8)

  // ForInfo
  if (isFloat) {
    writer.appendInt32(min)
  } else {
    writer.appendInt64(BigInt(min))
  }
  writer.appendUint8(bitWidth)

  packBits(writer, encoded, min, bitWidth)

  // exception positions, then raw bytes so values are copied bit-for-bit
  for (const pos of exceptions) {
    writer.appendUint8(pos & 0xff)
    writer.appendUint8(pos >> 8)
  }
  const size = values.BYTES_PER_ELEMENT
  const bytes = new Uint8Array(values.buffer, values.byteOffset, values.byteLength)
  for (const pos of exceptions) {
    writer.appendBytes(bytes.subarray(pos * size, (pos + 1) * size))
  }
}

/**
 * Decimal encode a value, or return undefined if it is an exception.
 *
 * @param {number} value
 * @param {number} e exponent
 * @param {number} f factor
 * @param {boolean} isFloat
 * @returns {number | undefined}
 */
function encode(value, e, f, isFloat) {
  if (!Number.isFinite(value) || Object.is(value, -0)) return
  const encoded = Math.round(value * POW10[e] * POW10_NEG[f])
  // check range and normative decode
  if (isFloat) {
    if (encoded < -0x80000000 || encoded > 0x7fffffff) return
    if (Math.fround(Math.fround(Math.fround(encoded) * POW10F[f]) * POW10F_NEG[e]) === value) return encoded
  } else {
    if (encoded < -TWO_63 || encoded >= TWO_63) return
    if (encoded * POW10[f] * POW10_NEG[e] === value) return encoded
  }
}

/**
 * Choose up to MAX_PRESETS (exponent, factor) candidates for the page by
 * exhaustive search over a few sampled vectors.
 *
 * @param {Float32Array | Float64Array} values
 * @param {number} numVectors
 * @returns {[number, number][]}
 */
function choosePresets(values, numVectors) {
  const maxExponent = values instanceof Float32Array ? 10 : 18
  /** @type {[number, number][]} */
  const all = []
  for (let e = 0; e <= maxExponent; e++) {
    for (let f = 0; f <= e; f++) {
      all.push([e, f])
    }
  }

  /** @type {Map<[number, number], number>} */
  const counts = new Map()
  const sampledVectors = Math.min(PRESET_VECTORS, numVectors)
  for (let i = 0; i < sampledVectors; i++) {
    const v = Math.floor(i * numVectors / sampledVectors)
    const best = bestExponentFactor(values.subarray(v * VECTOR_SIZE, (v + 1) * VECTOR_SIZE), all, PRESET_SAMPLE_SIZE)
    counts.set(best, (counts.get(best) ?? 0) + 1)
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, MAX_PRESETS)
    .map(([ef]) => ef)
}

/**
 * Choose the candidate that minimizes estimated size on a sample.
 *
 * @param {Float32Array | Float64Array} values
 * @param {[number, number][]} candidates
 * @param {number} sampleSize
 * @returns {[number, number]}
 */
function bestExponentFactor(values, candidates, sampleSize) {
  const isFloat = values instanceof Float32Array
  const sampleCount = Math.min(sampleSize, values.length)
  const step = values.length / sampleCount
  const exceptionBits = (2 + values.BYTES_PER_ELEMENT) * 8

  let best = candidates[0]
  let bestCost = Infinity
  for (const candidate of candidates) {
    const [e, f] = candidate
    let exceptions = 0
    let min = Infinity
    let max = -Infinity
    for (let s = 0; s < sampleCount; s++) {
      const enc = encode(values[Math.floor(s * step)], e, f, isFloat)
      if (enc === undefined) {
        exceptions++
      } else {
        if (enc < min) min = enc
        if (enc > max) max = enc
      }
    }
    const bitWidth = max > min ? bitLength(max - min) : 0
    const cost = sampleCount * bitWidth + exceptions * exceptionBits
    if (cost < bestCost) {
      bestCost = cost
      best = candidate
    }
  }
  return best
}

/**
 * Number of bits needed to represent a non-negative integer.
 *
 * @param {number} value
 * @returns {number}
 */
function bitLength(value) {
  if (value < TWO_32) return 32 - Math.clz32(value)
  return 32 + bitLength(Math.floor(value / TWO_32))
}

/**
 * Bit-pack unsigned deltas (encoded - min) LSB-first.
 * Writes ceil(count * bitWidth / 8) bytes.
 *
 * @param {Writer} writer
 * @param {Float64Array} encoded
 * @param {number} min frame of reference
 * @param {number} bitWidth
 */
function packBits(writer, encoded, min, bitWidth) {
  if (!bitWidth) return
  let buffer = 0
  let bits = 0

  /**
   * @param {number} value uint32
   * @param {number} width bits of value to write, at most 32
   */
  function pack(value, width) {
    while (width > 0) {
      const take = Math.min(8 - bits, width)
      buffer |= (value & (1 << take) - 1) << bits
      value >>>= take
      bits += take
      width -= take
      if (bits === 8) {
        writer.appendUint8(buffer)
        buffer = 0
        bits = 0
      }
    }
  }

  const loWidth = Math.min(bitWidth, 32)
  const hiWidth = bitWidth - loWidth
  for (const enc of encoded) {
    // subtraction is exact whenever the true delta is below 2^53
    const delta = enc - min
    if (Number.isSafeInteger(delta)) {
      pack(delta % TWO_32, loWidth)
      if (hiWidth) pack(Math.floor(delta / TWO_32), hiWidth)
    } else {
      const big = BigInt(enc) - BigInt(min)
      pack(Number(big & 0xffffffffn), loWidth)
      pack(Number(big >> 32n), hiWidth)
    }
  }
  if (bits) writer.appendUint8(buffer)
}
