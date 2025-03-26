
export interface Writer {
  buffer: ArrayBuffer
  offset: number
  getBuffer(): ArrayBuffer
  appendUint8(value: number): void
  appendUint32(value: number): void
  appendInt32(value: number): void
  appendInt64(value: bigint): void
  appendFloat64(value: number): void
  appendBuffer(buffer: ArrayBuffer): void
  appendBytes(value: Uint8Array): void
  appendVarInt(value: number): void
  appendVarBigInt(value: bigint): void
}
