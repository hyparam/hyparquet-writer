
/**
 * Self-expanding buffer view
 *
 * @import {Writer} from './types.js'
 * @returns {Writer}
 */
export function Writer() {
  this.buffer = new ArrayBuffer(1024)
  this.offset = 0
  this.view = new DataView(this.buffer)
  return this
}

/**
 * @param {number} size
 */
Writer.prototype.ensure = function(size) {
  if (this.offset + size > this.buffer.byteLength) {
    const newSize = Math.max(this.buffer.byteLength * 2, this.offset + size)
    const newBuffer = new ArrayBuffer(newSize)
    new Uint8Array(newBuffer).set(new Uint8Array(this.buffer))
    this.buffer = newBuffer
    this.view = new DataView(this.buffer)
  }
}

/**
 * @param {number} value
 */
Writer.prototype.appendUint8 = function(value) {
  this.ensure(this.offset + 1)
  this.view.setUint8(this.offset, value)
  this.offset++
}

/**
 * @param {number} value
 */
Writer.prototype.appendUint32 = function(value) {
  this.ensure(this.offset + 4)
  this.view.setUint32(this.offset, value, true)
  this.offset += 4
}

/**
 * @param {number} value
 */
Writer.prototype.appendFloat64 = function(value) {
  this.ensure(this.offset + 8)
  this.view.setFloat64(this.offset, value, true)
  this.offset += 8
}

/**
 * @param {ArrayBuffer} value
 */
Writer.prototype.appendBuffer = function(value) {
  this.ensure(this.offset + value.byteLength)
  new Uint8Array(this.buffer, this.offset, value.byteLength).set(new Uint8Array(value))
  this.offset += value.byteLength
}

/**
 * Convert a 32-bit signed integer to varint (1-5 bytes).
 * Writes out groups of 7 bits at a time, setting high bit if more to come.
 *
 * @param {number} value
 */
Writer.prototype.appendVarInt = function(value) {
  while (true) {
    if ((value & ~0x7f) === 0) {
      // fits in 7 bits
      this.appendUint8(value)
      return
    } else {
      // write 7 bits and set high bit
      this.appendUint8(value & 0x7f | 0x80)
      value >>>= 7
    }
  }
}

/**
 * Convert a bigint to varint (1-10 bytes for 64-bit range).
 *
 * @param {bigint} value
 */
Writer.prototype.appendVarBigInt = function(value) {
  while (true) {
    if ((value & ~0x7fn) === 0n) {
      // fits in 7 bits
      this.appendUint8(Number(value))
      return
    } else {
      // write 7 bits and set high bit
      this.appendUint8(Number(value & 0x7fn | 0x80n))
      value >>= 7n
    }
  }
}
