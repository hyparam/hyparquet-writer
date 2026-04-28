import { parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { ByteWriter, parquetWrite } from '../src/index.js'
import { exampleData } from './example.js'

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
})
