import type { ConvertedType, DecodedArray, FieldRepetitionType, KeyValue, LogicalType, ParquetType } from 'hyparquet'

export interface ParquetWriteOptions {
  writer: Writer
  columnData: ColumnData[]
  compressed?: boolean
  statistics?: boolean
  rowGroupSize?: number
  kvMetadata?: KeyValue[]
}

export interface ColumnData {
  name: string
  data: DecodedArray
  // fields from SchemaElement:
  type?: ParquetType
  type_length?: number
  repetition_type?: FieldRepetitionType
  converted_type?: ConvertedType
  scale?: number
  precision?: number
  field_id?: number
  logical_type?: LogicalType
}

export interface Writer {
  buffer: ArrayBuffer
  view: DataView
  offset: number

  ensure(size: number): void
  finish(): void
  getBuffer(): ArrayBuffer
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
}

export type ThriftObject = { [ key: `field_${number}` ]: ThriftType }
export type ThriftType = boolean | number | bigint | string | Uint8Array | ThriftType[] | ThriftObject | undefined
