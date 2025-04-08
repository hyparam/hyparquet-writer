import { createWriteStream, promises as fs } from 'fs'
import { pipeline } from 'stream/promises'
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects, parquetSchema } from 'hyparquet'
import { parquetWriteFile } from './src/write.js'

const url = 'https://s3.hyperparam.app/tpch-lineitem-v2.parquet'
const filename = 'data/tpch-lineitem-v2.parquet'

// download test parquet file if needed
let stat = await fs.stat(filename).catch(() => undefined)
if (!stat) {
  console.log('downloading ' + url)
  const res = await fetch(url)
  if (!res.ok) throw new Error(res.statusText)
  // write to file async
  await pipeline(res.body, createWriteStream(filename))
  stat = await fs.stat(filename)
  console.log('downloaded example.parquet', stat.size)
}

// asyncBuffer
const file = await asyncBufferFromFile(filename)
console.log(`parsing ${filename} (${stat.size.toLocaleString()} bytes)`)
let startTime = performance.now()

// read parquet file
const metadata = await parquetMetadataAsync(file)
const rows = await parquetReadObjects({
  file,
  metadata,
  columns: ['l_comment'],
  rowStart: 0,
  rowEnd: 100_000,
})
let ms = performance.now() - startTime
console.log(`parsed ${filename} ${rows.length.toLocaleString()} rows in ${ms.toFixed(0)} ms`)

// transpose rows
const schema = parquetSchema(metadata)
const columnData = schema.children.map(({ element }) => ({
  // name: element.name,
  // type: element.type,
  ...element,
  data: [],
})).filter(({ name }) => name === 'l_comment')
for (const row of rows) {
  for (const { name, data } of columnData) {
    data.push(row[name])
  }
}

// write parquet file
const outputFilename = 'data/output-tpch.parquet'
console.log(`writing ${outputFilename} (${rows.length.toLocaleString()} rows)`)
startTime = performance.now()
parquetWriteFile({
  filename: outputFilename,
  columnData,
})
ms = performance.now() - startTime
stat = await fs.stat(outputFilename)
console.log(`wrote ${outputFilename} (${stat.size.toLocaleString()} bytes) in ${ms.toFixed(0)} ms`)

// check data is the same
const outputFile = await asyncBufferFromFile(outputFilename)
const outputRows = await parquetReadObjects({ file: outputFile })
for (let i = 0; i < rows.length; i++) {
  const inputRow = JSON.stringify(rows[i])
  const outputRow = JSON.stringify(outputRows[i])
  if (inputRow !== outputRow) {
    console.log(`row ${i} mismatch`)
    console.log('input ', inputRow)
    console.log('output', outputRow)
  }
}
