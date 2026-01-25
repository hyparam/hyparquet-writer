/**
 * ALP (Adaptive Lossless floating-Point) encoding encoder.
 * Supports FLOAT and DOUBLE types.
 *
 * ALP encodes floating-point values by converting them to integers using
 * decimal scaling, then applying frame of reference (FOR) encoding and
 * bit-packing. Values that cannot be losslessly converted are stored as exceptions.
 */

const ALP_VERSION = 1
const ALP_COMPRESSION_MODE = 0 // ALP
const ALP_INTEGER_ENCODING = 0 // FOR+BitPack
const DEFAULT_LOG_VECTOR_SIZE = 10 // 1024 elements

// Precomputed powers of 10 for float (max exponent 10)
const POWERS_OF_10_FLOAT = new Float64Array([
  1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
  100000000, 1000000000, 10000000000,
])

// Precomputed powers of 10 for double (max exponent 18)
const POWERS_OF_10_DOUBLE = new Float64Array([
  1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
  100000000, 1000000000, 10000000000, 100000000000,
  1000000000000, 10000000000000, 100000000000000,
  1000000000000000, 10000000000000000, 100000000000000000,
  1000000000000000000,
])

// Integer ranges
const INT32_MIN = -2147483648
const INT32_MAX = 2147483647
const INT64_MIN = -9223372036854775808n
const INT64_MAX = 9223372036854775807n

/**
 * Write ALP encoded data.
 *
 * @import {DecodedArray, ParquetType} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer - output writer
 * @param {DecodedArray} values - values to encode
 * @param {ParquetType} type - FLOAT or DOUBLE
 */
export function writeALP(writer, values, type) {
  if (type === 'FLOAT') {
    writeALPFloat(writer, values)
  } else if (type === 'DOUBLE') {
    writeALPDouble(writer, values)
  } else {
    throw new Error(`ALP encoding unsupported type: ${type}`)
  }
}

/**
 * Write ALP encoded float data.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writeALPFloat(writer, values) {
  const numElements = values.length
  const vectorSize = 1 << DEFAULT_LOG_VECTOR_SIZE
  const numVectors = Math.ceil(numElements / vectorSize) || 1

  // Write header (8 bytes)
  writer.appendUint8(ALP_VERSION)
  writer.appendUint8(ALP_COMPRESSION_MODE)
  writer.appendUint8(ALP_INTEGER_ENCODING)
  writer.appendUint8(DEFAULT_LOG_VECTOR_SIZE)
  writer.appendInt32(numElements)

  // Process each vector to collect metadata
  /** @type {Array<{exponent: number, factor: number, numExceptions: number, encoded: Int32Array, exceptions: Array<{pos: number, value: number}>}>} */
  const vectorData = []

  for (let v = 0; v < numVectors; v++) {
    const start = v * vectorSize
    const end = Math.min(start + vectorSize, numElements)
    const vectorValues = values.slice(start, end)

    const { exponent, factor } = findBestExponentFactorFloat(vectorValues)
    const { encoded, exceptions } = encodeVectorFloat(vectorValues, exponent, factor)

    vectorData.push({ exponent, factor, numExceptions: exceptions.length, encoded, exceptions })
  }

  // Write AlpInfo array (4 bytes per vector)
  for (const { exponent, factor, numExceptions } of vectorData) {
    writer.appendUint8(exponent)
    writer.appendUint8(factor)
    writer.appendUint8(numExceptions & 0xFF)
    writer.appendUint8(numExceptions >> 8 & 0xFF)
  }

  // Write ForInfo array (5 bytes per vector for float)
  /** @type {Array<{frameOfReference: number, bitWidth: number, deltas: Int32Array}>} */
  const forData = []
  for (const { encoded } of vectorData) {
    const { frameOfReference, bitWidth, deltas } = computeFORFloat(encoded)
    forData.push({ frameOfReference, bitWidth, deltas })

    writer.appendInt32(frameOfReference)
    writer.appendUint8(bitWidth)
  }

  // Write Data array
  for (let v = 0; v < numVectors; v++) {
    const { exceptions } = vectorData[v]
    const { bitWidth, deltas } = forData[v]

    // Write bit-packed deltas
    if (bitWidth > 0) {
      packBitsFloat(writer, deltas, bitWidth)
    }

    // Write exception positions (uint16[])
    for (const { pos } of exceptions) {
      writer.appendUint8(pos & 0xFF)
      writer.appendUint8(pos >> 8 & 0xFF)
    }

    // Write exception values (float32[])
    for (const { value } of exceptions) {
      writer.appendFloat32(value)
    }
  }
}

/**
 * Write ALP encoded double data.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writeALPDouble(writer, values) {
  const numElements = values.length
  const vectorSize = 1 << DEFAULT_LOG_VECTOR_SIZE
  const numVectors = Math.ceil(numElements / vectorSize) || 1

  // Write header (8 bytes)
  writer.appendUint8(ALP_VERSION)
  writer.appendUint8(ALP_COMPRESSION_MODE)
  writer.appendUint8(ALP_INTEGER_ENCODING)
  writer.appendUint8(DEFAULT_LOG_VECTOR_SIZE)
  writer.appendInt32(numElements)

  // Process each vector to collect metadata
  /** @type {Array<{exponent: number, factor: number, numExceptions: number, encoded: BigInt64Array, exceptions: Array<{pos: number, value: number}>}>} */
  const vectorData = []

  for (let v = 0; v < numVectors; v++) {
    const start = v * vectorSize
    const end = Math.min(start + vectorSize, numElements)
    const vectorValues = values.slice(start, end)

    const { exponent, factor } = findBestExponentFactorDouble(vectorValues)
    const { encoded, exceptions } = encodeVectorDouble(vectorValues, exponent, factor)

    vectorData.push({ exponent, factor, numExceptions: exceptions.length, encoded, exceptions })
  }

  // Write AlpInfo array (4 bytes per vector)
  for (const { exponent, factor, numExceptions } of vectorData) {
    writer.appendUint8(exponent)
    writer.appendUint8(factor)
    writer.appendUint8(numExceptions & 0xFF)
    writer.appendUint8(numExceptions >> 8 & 0xFF)
  }

  // Write ForInfo array (9 bytes per vector for double)
  /** @type {Array<{frameOfReference: bigint, bitWidth: number, deltas: BigInt64Array}>} */
  const forData = []
  for (const { encoded } of vectorData) {
    const { frameOfReference, bitWidth, deltas } = computeFORDouble(encoded)
    forData.push({ frameOfReference, bitWidth, deltas })

    writer.appendInt64(frameOfReference)
    writer.appendUint8(bitWidth)
  }

  // Write Data array
  for (let v = 0; v < numVectors; v++) {
    const { exceptions } = vectorData[v]
    const { bitWidth, deltas } = forData[v]

    // Write bit-packed deltas
    if (bitWidth > 0) {
      packBitsDouble(writer, deltas, bitWidth)
    }

    // Write exception positions (uint16[])
    for (const { pos } of exceptions) {
      writer.appendUint8(pos & 0xFF)
      writer.appendUint8(pos >> 8 & 0xFF)
    }

    // Write exception values (float64[])
    for (const { value } of exceptions) {
      writer.appendFloat64(value)
    }
  }
}

/**
 * Find the best exponent/factor combination for float values.
 *
 * @param {DecodedArray} values
 * @returns {{exponent: number, factor: number}}
 */
function findBestExponentFactorFloat(values) {
  if (values.length === 0) {
    return { exponent: 0, factor: 0 }
  }

  const maxExponent = 10
  const sampleSize = Math.min(256, values.length)
  const samples = sampleValues(values, sampleSize)

  let bestE = 0
  let bestF = 0
  let bestExceptions = Infinity

  for (let e = 0; e <= maxExponent; e++) {
    for (let f = 0; f <= e; f++) {
      const exceptions = countExceptionsFloat(samples, e, f)
      if (exceptions < bestExceptions) {
        bestE = e
        bestF = f
        bestExceptions = exceptions
      }
      // Early exit if perfect
      if (exceptions === 0) {
        return { exponent: e, factor: f }
      }
    }
  }

  return { exponent: bestE, factor: bestF }
}

/**
 * Find the best exponent/factor combination for double values.
 *
 * @param {DecodedArray} values
 * @returns {{exponent: number, factor: number}}
 */
function findBestExponentFactorDouble(values) {
  if (values.length === 0) {
    return { exponent: 0, factor: 0 }
  }

  const maxExponent = 18
  const sampleSize = Math.min(256, values.length)
  const samples = sampleValues(values, sampleSize)

  let bestE = 0
  let bestF = 0
  let bestExceptions = Infinity

  for (let e = 0; e <= maxExponent; e++) {
    for (let f = 0; f <= e; f++) {
      const exceptions = countExceptionsDouble(samples, e, f)
      if (exceptions < bestExceptions) {
        bestE = e
        bestF = f
        bestExceptions = exceptions
      }
      // Early exit if perfect
      if (exceptions === 0) {
        return { exponent: e, factor: f }
      }
    }
  }

  return { exponent: bestE, factor: bestF }
}

/**
 * Sample values from an array.
 *
 * @param {DecodedArray} values
 * @param {number} sampleSize
 * @returns {DecodedArray}
 */
function sampleValues(values, sampleSize) {
  if (values.length <= sampleSize) {
    return values
  }

  const samples = []
  const step = values.length / sampleSize
  for (let i = 0; i < sampleSize; i++) {
    samples.push(values[Math.floor(i * step)])
  }
  return samples
}

/**
 * Count exceptions for a given exponent/factor combination (float).
 *
 * @param {DecodedArray} values
 * @param {number} exponent
 * @param {number} factor
 * @returns {number}
 */
function countExceptionsFloat(values, exponent, factor) {
  let count = 0
  const multiplier = POWERS_OF_10_FLOAT[exponent] / POWERS_OF_10_FLOAT[factor]
  const divisor = POWERS_OF_10_FLOAT[factor] / POWERS_OF_10_FLOAT[exponent]

  for (const value of values) {
    if (isExceptionFloat(value, multiplier, divisor)) {
      count++
    }
  }
  return count
}

/**
 * Count exceptions for a given exponent/factor combination (double).
 *
 * @param {DecodedArray} values
 * @param {number} exponent
 * @param {number} factor
 * @returns {number}
 */
function countExceptionsDouble(values, exponent, factor) {
  let count = 0
  const multiplier = POWERS_OF_10_DOUBLE[exponent] / POWERS_OF_10_DOUBLE[factor]
  const divisor = POWERS_OF_10_DOUBLE[factor] / POWERS_OF_10_DOUBLE[exponent]

  for (const value of values) {
    if (isExceptionDouble(value, multiplier, divisor)) {
      count++
    }
  }
  return count
}

/**
 * Check if a float value is an exception.
 *
 * @param {number} value
 * @param {number} multiplier - 10^exponent / 10^factor
 * @param {number} divisor - 10^factor / 10^exponent
 * @returns {boolean}
 */
function isExceptionFloat(value, multiplier, divisor) {
  // NaN, Inf, -Inf
  if (!Number.isFinite(value)) return true
  // Negative zero
  if (Object.is(value, -0)) return true

  // Encode using magic number rounding
  const scaled = value * multiplier
  const encoded = Math.round(scaled)

  // Check overflow
  if (encoded < INT32_MIN || encoded > INT32_MAX) return true

  // Round-trip check
  const decoded = encoded * divisor
  // Use Math.fround to ensure float32 precision comparison
  return Math.fround(decoded) !== Math.fround(value)
}

/**
 * Check if a double value is an exception.
 *
 * @param {number} value
 * @param {number} multiplier - 10^exponent / 10^factor
 * @param {number} divisor - 10^factor / 10^exponent
 * @returns {boolean}
 */
function isExceptionDouble(value, multiplier, divisor) {
  // NaN, Inf, -Inf
  if (!Number.isFinite(value)) return true
  // Negative zero
  if (Object.is(value, -0)) return true

  // Encode
  const scaled = value * multiplier
  const encoded = Math.round(scaled)

  // Check overflow (JavaScript number precision limits this)
  if (encoded < Number(INT64_MIN) || encoded > Number(INT64_MAX)) return true

  // Round-trip check
  const decoded = encoded * divisor
  return decoded !== value
}

/**
 * Encode a vector of float values.
 *
 * @param {DecodedArray} values
 * @param {number} exponent
 * @param {number} factor
 * @returns {{encoded: Int32Array, exceptions: Array<{pos: number, value: number}>}}
 */
function encodeVectorFloat(values, exponent, factor) {
  const encoded = new Int32Array(values.length)
  /** @type {Array<{pos: number, value: number}>} */
  const exceptions = []

  const multiplier = POWERS_OF_10_FLOAT[exponent] / POWERS_OF_10_FLOAT[factor]
  const divisor = POWERS_OF_10_FLOAT[factor] / POWERS_OF_10_FLOAT[exponent]

  // Find first non-exception value for placeholder
  let placeholder = 0
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (!isExceptionFloat(value, multiplier, divisor)) {
      placeholder = Math.round(value * multiplier)
      break
    }
  }

  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (isExceptionFloat(value, multiplier, divisor)) {
      encoded[i] = placeholder
      exceptions.push({ pos: i, value })
    } else {
      encoded[i] = Math.round(value * multiplier)
    }
  }

  return { encoded, exceptions }
}

/**
 * Encode a vector of double values.
 *
 * @param {DecodedArray} values
 * @param {number} exponent
 * @param {number} factor
 * @returns {{encoded: BigInt64Array, exceptions: Array<{pos: number, value: number}>}}
 */
function encodeVectorDouble(values, exponent, factor) {
  const encoded = new BigInt64Array(values.length)
  /** @type {Array<{pos: number, value: number}>} */
  const exceptions = []

  const multiplier = POWERS_OF_10_DOUBLE[exponent] / POWERS_OF_10_DOUBLE[factor]
  const divisor = POWERS_OF_10_DOUBLE[factor] / POWERS_OF_10_DOUBLE[exponent]

  // Find first non-exception value for placeholder
  let placeholder = 0n
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (!isExceptionDouble(value, multiplier, divisor)) {
      placeholder = BigInt(Math.round(value * multiplier))
      break
    }
  }

  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (isExceptionDouble(value, multiplier, divisor)) {
      encoded[i] = placeholder
      exceptions.push({ pos: i, value })
    } else {
      encoded[i] = BigInt(Math.round(value * multiplier))
    }
  }

  return { encoded, exceptions }
}

/**
 * Compute Frame of Reference encoding for float (Int32).
 *
 * @param {Int32Array} encoded
 * @returns {{frameOfReference: number, bitWidth: number, deltas: Int32Array}}
 */
function computeFORFloat(encoded) {
  if (encoded.length === 0) {
    return { frameOfReference: 0, bitWidth: 0, deltas: new Int32Array(0) }
  }

  // Find min value
  let min = encoded[0]
  for (let i = 1; i < encoded.length; i++) {
    if (encoded[i] < min) min = encoded[i]
  }

  // Compute deltas
  const deltas = new Int32Array(encoded.length)
  let max = 0
  for (let i = 0; i < encoded.length; i++) {
    deltas[i] = encoded[i] - min
    if (deltas[i] > max) max = deltas[i]
  }

  // Compute bit width
  const bitWidth = max === 0 ? 0 : Math.ceil(Math.log2(max + 1))

  return { frameOfReference: min, bitWidth, deltas }
}

/**
 * Compute Frame of Reference encoding for double (BigInt64).
 *
 * @param {BigInt64Array} encoded
 * @returns {{frameOfReference: bigint, bitWidth: number, deltas: BigInt64Array}}
 */
function computeFORDouble(encoded) {
  if (encoded.length === 0) {
    return { frameOfReference: 0n, bitWidth: 0, deltas: new BigInt64Array(0) }
  }

  // Find min value
  let min = encoded[0]
  for (let i = 1; i < encoded.length; i++) {
    if (encoded[i] < min) min = encoded[i]
  }

  // Compute deltas
  const deltas = new BigInt64Array(encoded.length)
  let max = 0n
  for (let i = 0; i < encoded.length; i++) {
    deltas[i] = encoded[i] - min
    if (deltas[i] > max) max = deltas[i]
  }

  // Compute bit width
  let bitWidth = 0
  if (max > 0n) {
    let temp = max
    while (temp > 0n) {
      bitWidth++
      temp >>= 1n
    }
  }

  return { frameOfReference: min, bitWidth, deltas }
}

/**
 * Bit-pack Int32 deltas.
 *
 * @param {Writer} writer
 * @param {Int32Array} deltas
 * @param {number} bitWidth
 */
function packBitsFloat(writer, deltas, bitWidth) {
  if (bitWidth === 0) return // All zeros, no data

  const mask = (1 << bitWidth) - 1
  let buffer = 0
  let bitsInBuffer = 0

  for (const delta of deltas) {
    buffer |= (delta & mask) << bitsInBuffer
    bitsInBuffer += bitWidth

    while (bitsInBuffer >= 8) {
      writer.appendUint8(buffer & 0xFF)
      buffer >>>= 8
      bitsInBuffer -= 8
    }
  }

  // Flush remaining bits
  if (bitsInBuffer > 0) {
    writer.appendUint8(buffer & 0xFF)
  }
}

/**
 * Bit-pack BigInt64 deltas.
 *
 * @param {Writer} writer
 * @param {BigInt64Array} deltas
 * @param {number} bitWidth
 */
function packBitsDouble(writer, deltas, bitWidth) {
  if (bitWidth === 0) return // All zeros, no data

  const mask = (1n << BigInt(bitWidth)) - 1n
  let buffer = 0n
  let bitsInBuffer = 0

  for (const delta of deltas) {
    buffer |= (delta & mask) << BigInt(bitsInBuffer)
    bitsInBuffer += bitWidth

    while (bitsInBuffer >= 8) {
      writer.appendUint8(Number(buffer & 0xFFn))
      buffer >>= 8n
      bitsInBuffer -= 8
    }
  }

  // Flush remaining bits
  if (bitsInBuffer > 0) {
    writer.appendUint8(Number(buffer & 0xFFn))
  }
}
