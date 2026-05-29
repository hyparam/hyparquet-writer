import type { ColumnChunk, ColumnIndex, CompressionCodec, DecodedArray, Encoding, KeyValue, OffsetIndex, SchemaElement } from 'hyparquet'

export type Compressor = (input: Uint8Array) => Uint8Array
export type Compressors = { [K in CompressionCodec]?: Compressor }

// Superset of parquet types with automatic conversions
export type BasicType =
  'BOOLEAN' |
  'INT32' |
  'INT64' |
  'FLOAT' |
  'DOUBLE' |
  'BYTE_ARRAY' |
  'STRING' |
  'JSON' |
  'TIMESTAMP' |
  'UUID' |
  'FLOAT16' |
  'GEOMETRY' |
  'GEOGRAPHY' |
  'VARIANT'

// Recursive variant shredding config: a scalar BasicType, an array of one
// element shred type ([elem] = array-of-elem), or an object mapping field names
// to shred types. The single-element array is a shape template; only index 0 is read.
export type ShredType = BasicType | ShredType[] | { [field: string]: ShredType }

export interface ParquetWriteOptions {
  writer: Writer
  columnData: ColumnSource[]
  schema?: SchemaElement[]
  codec?: CompressionCodec // global default codec, default 'SNAPPY'
  compressors?: Compressors // custom compressors
  statistics?: boolean // enable column statistics, default true
  rowGroupSize?: number | number[] // number of rows per row group
  pageSize?: number // target uncompressed page size in bytes, default 1048576
  kvMetadata?: KeyValue[]
}

export interface ColumnSource {
  name: string
  data: DecodedArray
  type?: BasicType
  nullable?: boolean
  encoding?: Encoding
  codec?: CompressionCodec // per-column codec override, default ParquetWriteOptions.codec
  columnIndex?: boolean // write column indexes, default false
  offsetIndex?: boolean // write offset indexes, default true
  shredding?: true | ShredType // variant shredding config (true = auto-detect)
  bloomFilter?: boolean | BloomFilterOptions // write bloom filter, default false
}

export interface PageData {
  values: DecodedArray
  definitionLevels: number[]
  repetitionLevels: number[]
  maxDefinitionLevel: number
}

export interface BloomFilterOptions {
  fpp?: number // false positive probability, default 0.01
  maxBytes?: number // skip emission above this size, default 1 MiB
}

export interface ColumnEncoder {
  columnName: string
  element: SchemaElement
  schemaPath: SchemaElement[]
  codec: CompressionCodec
  compressors: Compressors
  stats: boolean
  pageSize: number
  // Spec: If ColumnIndex is present, OffsetIndex must also be present
  columnIndex: boolean
  offsetIndex: boolean
  encoding?: Encoding // user-specified encoding
  bloomFilter?: boolean | BloomFilterOptions
}

export interface PageIndexes {
  chunk: ColumnChunk
  columnIndex?: ColumnIndex
  offsetIndex?: OffsetIndex
  bloomFilter?: Uint32Array // finalized SBBF blocks
}

export interface Writer {
  buffer: ArrayBuffer
  view: DataView
  offset: number // total bytes written

  ensure(size: number): void
  flush?(): void | Promise<void>
  finish(): void | Promise<void>
  getBuffer(): ArrayBuffer
  getBytes(): Uint8Array
  appendUint8(value: number): void
  appendUint32(value: number): void
  appendInt32(value: number): void
  appendInt64(value: bigint): void
  appendFloat32(value: number): void
  appendFloat64(value: number): void
  appendBuffer(buffer: ArrayBuffer): void
  appendBytes(value: Uint8Array): void
  appendVarInt(value: number): void
  appendVarBigInt(value: bigint): void
  appendZigZag(value: number | bigint): void
}

export type ThriftObject = { [ key: `field_${number}` ]: ThriftType }
export type ThriftType = boolean | number | bigint | string | Uint8Array | ThriftType[] | ThriftObject | undefined
