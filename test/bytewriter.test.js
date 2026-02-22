import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'

describe('ByteWriter', () => {
  it('initializes with correct defaults', () => {
    const writer = new ByteWriter()
    expect(writer.offset).toBe(0)
    expect(writer.buffer.byteLength).toBe(1024)
  })

  it('appendUint8 writes single byte', () => {
    const writer = new ByteWriter()
    writer.appendUint8(255)
    expect(writer.getBytes()).toEqual(new Uint8Array([0xff]))
  })

  it('appendUint32 writes a 32-bit integer in little-endian', () => {
    const writer = new ByteWriter()
    writer.appendUint32(0x12345678)
    expect(writer.getBytes()).toEqual(
      new Uint8Array([0x78, 0x56, 0x34, 0x12])
    )
  })

  it('appendInt32 writes signed 32-bit integer in little-endian', () => {
    const writer = new ByteWriter()
    writer.appendInt32(-1)
    expect(writer.getBytes()).toEqual(
      new Uint8Array([0xff, 0xff, 0xff, 0xff])
    )
  })

  it('appendInt64 writes a 64-bit bigint in little-endian', () => {
    const writer = new ByteWriter()
    writer.appendInt64(0x1122334455667788n)
    expect(writer.getBytes()).toEqual(
      new Uint8Array([0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11])
    )
  })

  it('appendFloat64 writes a 64-bit float in little-endian', () => {
    const writer = new ByteWriter()
    writer.appendFloat64(1.0)
    expect(writer.getBytes()).toEqual(
      new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f])
    )
  })

  it('appendBytes writes raw Uint8Array data', () => {
    const writer = new ByteWriter()
    writer.appendBytes(new Uint8Array([1, 2, 3, 4]))
    expect(writer.getBytes()).toEqual(new Uint8Array([1, 2, 3, 4]))
  })

  it('appendBuffer writes raw ArrayBuffer data', () => {
    const writer = new ByteWriter()
    const buf = new Uint8Array([10, 20, 30]).buffer
    writer.appendBuffer(buf)
    expect(writer.getBytes()).toEqual(new Uint8Array([10, 20, 30]))
  })

  it('appendVarInt encodes 32-bit varint', () => {
    const writer = new ByteWriter()
    writer.appendVarInt(127)
    writer.appendVarInt(128)
    writer.appendVarInt(300)

    expect(writer.getBytes()).toEqual(
      new Uint8Array([
        0x7f, // 127
        0x80, 0x01, // 128
        0xac, 0x02, // 300
      ])
    )
  })

  it('appendVarBigInt encodes bigint varint', () => {
    const writer = new ByteWriter()
    writer.appendVarBigInt(127n)
    writer.appendVarBigInt(128n)
    writer.appendVarBigInt(300n)

    expect(writer.getBytes()).toEqual(
      new Uint8Array([
        0x7f, // 127
        0x80, 0x01, // 128
        0xac, 0x02, // 300
      ])
    )
  })

  it('expands buffer automatically when needed', () => {
    const writer = new ByteWriter()
    // force expansion by writing more than initial 1024 bytes
    const largeArray = new Uint8Array(2000).fill(0xaa)
    writer.appendBytes(largeArray)
    expect(writer.buffer.byteLength).toBeGreaterThanOrEqual(2000)
    expect(writer.getBytes().length).toBe(2000)
  })

  it('finish does nothing but is callable', () => {
    const writer = new ByteWriter()
    writer.finish()
    expect(writer.getBytes().byteLength).toBe(0)
  })
})
