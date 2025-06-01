import fs from 'fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { fileWriter } from '../src/node.js'

const filedir = 'data/'
const filename = 'data/filewriter.test.bin'

describe('FileWriter', () => {
  beforeEach(() => {
    // ensure data directory exists
    if (!fs.existsSync(filedir)) {
      fs.mkdirSync(filedir)
    }
  })

  afterEach(() => {
    // remove test file
    if (fs.existsSync(filename)) {
      fs.unlinkSync(filename)
    }
  })

  it('throws an error when calling getBuffer', () => {
    const writer = fileWriter(filename)
    expect(() => writer.getBuffer()).toThrowError('getBuffer not supported')
  })

  it('writes single byte and flushes on finish', () => {
    const writer = fileWriter(filename)
    writer.appendUint8(0xff)
    writer.finish()

    // verify file exists and content is correct
    expect(fs.existsSync(filename)).toBe(true)
    const contents = fs.readFileSync(filename)
    expect(new Uint8Array(contents)).toEqual(new Uint8Array([0xff]))
  })

  it('writes multiple data types to file', () => {
    const writer = fileWriter(filename)
    writer.appendUint8(0xab)
    writer.appendUint32(0x12345678)
    writer.appendInt32(-1)
    writer.appendInt64(0x1122334455667788n)
    writer.appendVarInt(300)
    writer.finish()

    const contents = new Uint8Array(fs.readFileSync(filename))

    const expected = new Uint8Array([
      0xab,
      0x78, 0x56, 0x34, 0x12,
      0xff, 0xff, 0xff, 0xff,
      0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
      0xac, 0x02,
    ])
    expect(contents).toEqual(expected)
  })

  it('auto-flushes when exceeding chunk size', () => {
    // default chunkSize = 1_000_000 bytes
    const writer = fileWriter(filename)

    // write slightly over 1mb to trigger auto-flush
    const largeArray = new Uint8Array(1_100_000).fill(0xaa)
    writer.appendBytes(largeArray)
    writer.appendBytes(largeArray)

    // expect first flush
    expect(fs.statSync(filename).size).toBe(1_100_000)

    writer.finish()

    // expect final flush
    expect(fs.statSync(filename).size).toBe(2_200_000)
  })

  it('overwrites existing file if new writer is created with same filename', () => {
    // first write
    let writer = fileWriter(filename)
    writer.appendBytes(new Uint8Array([0x11, 0x22]))
    writer.finish()

    // verify the file now has [0x11, 0x22]
    let contents = fs.readFileSync(filename)
    expect(new Uint8Array(contents)).toEqual(new Uint8Array([0x11, 0x22]))

    // second write
    writer = fileWriter(filename)
    writer.appendBytes(new Uint8Array([0xaa, 0xbb]))
    writer.finish()

    // should overwrite the previous content
    contents = fs.readFileSync(filename)
    expect(new Uint8Array(contents)).toEqual(new Uint8Array([0xaa, 0xbb]))
  })
})
