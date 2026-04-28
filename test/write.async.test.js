import { parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { ByteWriter, parquetWrite } from '../src/index.js'
import { exampleData } from './example.js'

/**
 * @import {Writer} from '../src/types.js'
 */

describe('parquetWrite async finish', () => {
  it('awaits a writer whose finish() returns a Promise', async () => {
    const writer = new ByteWriter()
    let finished = false
    writer.finish = async () => {
      await Promise.resolve()
      finished = true
    }

    const result = parquetWrite({ writer, columnData: exampleData })
    expect(result).toBeInstanceOf(Promise)
    expect(finished).toBe(false)
    await result
    expect(finished).toBe(true)

    const output = await parquetReadObjects({ file: writer.getBuffer() })
    expect(output).toHaveLength(4)
  })

  it('stays synchronous when writer.finish() is sync', () => {
    const writer = new ByteWriter()
    const result = parquetWrite({ writer, columnData: exampleData })
    expect(result).toBeUndefined()
  })

  it('calls flush() between row groups and awaits returned promises', async () => {
    /** @type {Writer} */
    const writer = new ByteWriter()
    /** @type {number[]} */
    const flushOffsets = []
    let resolved = 0
    writer.flush = async () => {
      flushOffsets.push(writer.offset)
      await Promise.resolve()
      resolved++
    }

    const columnData = [{ name: 'n', data: Array.from({ length: 2500 }, (_, i) => i) }]
    // rowGroupSize=1000 → 3 row groups → 3 flush calls
    const result = parquetWrite({ writer, columnData, rowGroupSize: 1000 })
    expect(result).toBeInstanceOf(Promise)
    await result

    expect(flushOffsets).toHaveLength(3)
    expect(resolved).toBe(3)
    // each flush sees a strictly larger offset (data accumulated between groups)
    expect(flushOffsets[0]).toBeLessThan(flushOffsets[1])
    expect(flushOffsets[1]).toBeLessThan(flushOffsets[2])

    const output = await parquetReadObjects({ file: writer.getBuffer() })
    expect(output).toHaveLength(2500)
  })
})
