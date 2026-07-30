/**
 * @import {Writer} from '../src/types.js'
 */

/**
 * Writes data to an auto-expanding ArrayBuffer.
 *
 * @implements {Writer}
 */
export class ByteWriter {
  /**
   * @param {number} [initalSize]
   */
  constructor(initalSize = 1024) {
    this.buffer = new ArrayBuffer(initalSize)
    this.view = new DataView(this.buffer)
    this.offset = 0 // total bytes written
    this.index = 0 // index in buffer (may be reset when flushing to file)
  }

  /**
   * @param {number} size
   */
  ensure(size) {
    // auto-expanding buffer
    if (this.index + size > this.buffer.byteLength) {
      const newSize = Math.max(this.buffer.byteLength * 2, this.index + size)
      const newBuffer = new ArrayBuffer(newSize)
      // TODO: save buffers until later and merge once?
      new Uint8Array(newBuffer).set(new Uint8Array(this.buffer))
      this.buffer = newBuffer
      this.view = new DataView(this.buffer)
    }
  }

  finish() {
  }

  getBuffer() {
    return this.buffer.slice(0, this.index)
  }

  getBytes() {
    return new Uint8Array(this.buffer, 0, this.index)
  }

  /**
   * @param {number} value
   */
  appendUint8(value) {
    this.ensure(this.index + 1)
    this.view.setUint8(this.index, value)
    this.offset++
    this.index++
  }

  /**
   * @param {number} value
   */
  appendUint32(value) {
    this.ensure(this.index + 4)
    this.view.setUint32(this.index, value, true)
    this.offset += 4
    this.index += 4
  }

  /**
   * @param {number} value
   */
  appendInt32(value) {
    this.ensure(this.index + 4)
    this.view.setInt32(this.index, value, true)
    this.offset += 4
    this.index += 4
  }

  /**
   * @param {bigint} value
   */
  appendInt64(value) {
    this.ensure(this.index + 8)
    this.view.setBigInt64(this.index, BigInt(value), true)
    this.offset += 8
    this.index += 8
  }

  /**
   * @param {number} value
   */
  appendFloat32(value) {
    this.ensure(this.index + 8)
    this.view.setFloat32(this.index, value, true)
    this.offset += 4
    this.index += 4
  }

  /**
   * @param {number} value
   */
  appendFloat64(value) {
    this.ensure(this.index + 8)
    this.view.setFloat64(this.index, value, true)
    this.offset += 8
    this.index += 8
  }

  /**
   * @param {ArrayBuffer} value
   */
  appendBuffer(value) {
    this.appendBytes(new Uint8Array(value))
  }

  /**
   * @param {Uint8Array} value
   */
  appendBytes(value) {
    this.ensure(this.index + value.length)
    new Uint8Array(this.buffer, this.index, value.length).set(value)
    this.offset += value.length
    this.index += value.length
  }

  /**
   * Convert a 32-bit signed integer to varint (1-5 bytes).
   * Writes out groups of 7 bits at a time, setting high bit if more to come.
   *
   * @param {number} value
   */
  appendVarInt(value) {
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
  appendVarBigInt(value) {
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

  /**
   * Convert number to zigzag encoding and write as varint.
   *
   * @param {number | bigint} value
   */
  appendZigZag(value) {
    if (typeof value === 'number') {
      this.appendVarInt(value << 1 ^ value >> 31)
    } else {
      this.appendVarBigInt(value << 1n ^ value >> 63n)
    }
  }
}
