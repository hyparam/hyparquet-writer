/**
 * @typedef {Object} Writer
 * @property {ArrayBuffer} buffer - The buffer being written to
 * @property {DataView} view - View of the buffer
 * @property {number} offset - Number of bytes written
 * @property {number} index - Current index in buffer
 * @property {function(number): void} ensure - Ensure buffer has space
 * @property {function(): void} finish - Finish writing
 * @property {function(): ArrayBuffer} getBuffer - Get the written buffer
 * @property {function(number): void} appendUint8 - Write uint8
 * @property {function(number): void} appendUint32 - Write uint32
 * @property {function(number): void} appendInt32 - Write int32
 * @property {function(bigint): void} appendInt64 - Write int64
 * @property {function(number): void} appendFloat32 - Write float32
 * @property {function(number): void} appendFloat64 - Write float64
 * @property {function(ArrayBuffer): void} appendBuffer - Write buffer
 * @property {function(Uint8Array): void} appendBytes - Write bytes
 * @property {function(number): void} appendVarInt - Write varint
 * @property {function(bigint): void} appendVarBigInt - Write varint for bigint
 */

/**
 * @typedef {('BOOLEAN'|'INT32'|'INT64'|'INT96'|'FLOAT'|'DOUBLE'|'BYTE_ARRAY'|'FIXED_LEN_BYTE_ARRAY')} ParquetType
 */

/**
 * @typedef {('UNCOMPRESSED'|'SNAPPY'|'GZIP'|'LZO'|'BROTLI'|'LZ4'|'ZSTD'|'LZ4_RAW')} CompressionCodec
 */

/**
 * @typedef {('REQUIRED'|'OPTIONAL'|'REPEATED')} FieldRepetitionType
 */

/**
 * @typedef {('PLAIN'|'GROUP_VAR_INT'|'PLAIN_DICTIONARY'|'RLE'|'BIT_PACKED'|'DELTA_BINARY_PACKED'|'DELTA_LENGTH_BYTE_ARRAY'|'DELTA_BYTE_ARRAY'|'RLE_DICTIONARY'|'BYTE_STREAM_SPLIT')} Encoding
 */

/**
 * @typedef {('UTF8'|'MAP'|'MAP_KEY_VALUE'|'LIST'|'ENUM'|'DECIMAL'|'DATE'|'TIME_MILLIS'|'TIME_MICROS'|'TIMESTAMP_MILLIS'|'TIMESTAMP_MICROS'|'UINT_8'|'UINT_16'|'UINT_32'|'UINT_64'|'INT_8'|'INT_16'|'INT_32'|'INT_64'|'JSON'|'BSON'|'INTERVAL')} ConvertedType
 */

/**
 * @typedef {('MILLIS'|'MICROS'|'NANOS')} TimeUnit
 */

/**
 * @typedef {('STRING'|'MAP'|'LIST'|'ENUM'|'DATE'|'INTERVAL'|'NULL'|'JSON'|'BSON'|'UUID'|'FLOAT16'|'VARIANT'|'GEOMETRY'|'GEOGRAPHY')} LogicalTypeSimple
 */

/**
 * @typedef {{type: LogicalTypeSimple}|{type: 'DECIMAL', precision: number, scale: number}|{type: 'TIME', isAdjustedToUTC: boolean, unit: TimeUnit}|{type: 'TIMESTAMP', isAdjustedToUTC: boolean, unit: TimeUnit}|{type: 'INTEGER', bitWidth: number, isSigned: boolean}} LogicalType
 */

/**
 * @typedef {Object} SchemaElement
 * @property {string} name - Name of the element
 * @property {ParquetType} [type] - Type of the element
 * @property {number} [num_children] - Number of children
 * @property {FieldRepetitionType} [repetition_type] - Repetition type
 * @property {ConvertedType} [converted_type] - Logical type
 * @property {LogicalType} [logical_type] - Logical type
 * @property {number} [type_length] - Length for FIXED_LEN_BYTE_ARRAY
 * @property {number} [precision] - Precision for DECIMAL
 * @property {number} [scale] - Scale for DECIMAL
 * @property {number} [field_id] - Field ID
 */

/**
 * @typedef {string|number|bigint|boolean|Date|Uint8Array} MinMaxType
 */

/**
 * @typedef {Object} Statistics
 * @property {MinMaxType} [max_value] - Maximum value
 * @property {MinMaxType} [min_value] - Minimum value
 * @property {bigint} [null_count] - Number of null values
 * @property {bigint} [distinct_count] - Number of distinct values
 * @property {boolean} [is_sorted] - Whether values are sorted
 */

/**
 * @typedef {(Uint8Array|Uint32Array|Int32Array|BigInt64Array|BigUint64Array|Float32Array|Float64Array|any[])} DecodedArray
 */

/**
 * @typedef {Object} ColumnData
 * @property {string} name - Column name
 * @property {DecodedArray} data - Column data
 * @property {ParquetType} [type] - Column type
 * @property {FieldRepetitionType} [repetition_type] - Column repetition type
 * @property {ConvertedType} [converted_type] - Column logical type
 * @property {number} [type_length] - Type length for FIXED_LEN_BYTE_ARRAY
 */

/**
 * @typedef {Object} KeyValue
 * @property {string} key - Key
 * @property {string} [value] - Value
 */

/**
 * @typedef {Object} SizeStatistics
 * @property {bigint} [unencoded_byte_array_data_bytes] - Unencoded byte array data bytes
 * @property {bigint[]} [repetition_level_histogram] - Repetition level histogram
 * @property {bigint[]} [definition_level_histogram] - Definition level histogram
 */

/**
 * @typedef {Object} PageEncodingStats
 * @property {PageType} page_type - Page type
 * @property {Encoding} encoding - Encoding
 * @property {number} count - Count
 */

/**
 * @typedef {Object} ColumnMetaData
 * @property {ParquetType} type - Type of column
 * @property {Encoding[]} encodings - Encodings used
 * @property {string[]} path_in_schema - Path in schema
 * @property {CompressionCodec} codec - Compression codec
 * @property {bigint} num_values - Number of values
 * @property {bigint} total_uncompressed_size - Total uncompressed size
 * @property {bigint} total_compressed_size - Total compressed size
 * @property {bigint} data_page_offset - Offset of first data page
 * @property {bigint} [dictionary_page_offset] - Offset of dictionary page
 * @property {Statistics} [statistics] - Column statistics
 * @property {KeyValue[]} [key_value_metadata] - Key-value metadata
 * @property {bigint} [index_page_offset] - Index page offset
 * @property {PageEncodingStats[]} [encoding_stats] - Encoding stats
 * @property {bigint} [bloom_filter_offset] - Bloom filter offset
 * @property {number} [bloom_filter_length] - Bloom filter length
 * @property {SizeStatistics} [size_statistics] - Size statistics
 */

/**
 * @typedef {Object} SortingColumn
 * @property {number} column_idx - Column index
 * @property {boolean} descending - Whether to sort descending
 * @property {boolean} nulls_first - Whether nulls come first
 */

/**
 * @typedef {Object} ColumnChunk
 * @property {bigint} file_offset - Offset in file
 * @property {string} [file_path] - File path
 * @property {ColumnMetaData} [meta_data] - Column metadata
 * @property {bigint} [offset_index_offset] - Offset index offset
 * @property {number} [offset_index_length] - Offset index length
 * @property {bigint} [column_index_offset] - Column index offset
 * @property {number} [column_index_length] - Column index length
 * @property {Uint8Array} [encrypted_column_metadata] - Encrypted column metadata
 */

/**
 * @typedef {Object} RowGroup
 * @property {ColumnChunk[]} columns - Column chunks
 * @property {bigint} total_byte_size - Total byte size
 * @property {bigint} num_rows - Number of rows
 * @property {SortingColumn[]} [sorting_columns] - Sorting columns
 * @property {bigint} [file_offset] - File offset
 * @property {bigint} [total_compressed_size] - Total compressed size
 */

/**
 * @typedef {Object} FileMetaData
 * @property {number} version - File version
 * @property {string} [created_by] - Created by
 * @property {SchemaElement[]} schema - File schema
 * @property {bigint} num_rows - Number of rows
 * @property {RowGroup[]} row_groups - Row groups
 * @property {number} [metadata_length] - Metadata length
 * @property {KeyValue[]} [key_value_metadata] - Key-value metadata
 */

/**
 * @typedef {('DATA_PAGE'|'INDEX_PAGE'|'DICTIONARY_PAGE'|'DATA_PAGE_V2')} PageType
 */

/**
 * @typedef {Object} DataPageHeader
 * @property {number} num_values - Number of values
 * @property {Encoding} encoding - Encoding
 * @property {Encoding} definition_level_encoding - Definition level encoding
 * @property {Encoding} repetition_level_encoding - Repetition level encoding
 * @property {Statistics} [statistics] - Statistics
 */

/**
 * @typedef {Object} DictionaryPageHeader
 * @property {number} num_values - Number of values
 * @property {Encoding} encoding - Encoding
 * @property {boolean} [is_sorted] - Whether values are sorted
 */

/**
 * @typedef {Object} DataPageHeaderV2
 * @property {number} num_values - Number of values
 * @property {number} num_nulls - Number of nulls
 * @property {number} num_rows - Number of rows
 * @property {Encoding} encoding - Encoding
 * @property {number} definition_levels_byte_length - Definition levels byte length
 * @property {number} repetition_levels_byte_length - Repetition levels byte length
 * @property {boolean} [is_compressed] - Whether data is compressed
 * @property {Statistics} [statistics] - Statistics
 */

/**
 * @typedef {Object} PageHeader
 * @property {PageType} type - Page type
 * @property {number} uncompressed_page_size - Uncompressed size
 * @property {number} compressed_page_size - Compressed size
 * @property {number} [crc] - CRC
 * @property {DataPageHeader} [data_page_header] - Data page header
 * @property {DictionaryPageHeader} [dictionary_page_header] - Dictionary page header
 * @property {DataPageHeaderV2} [data_page_header_v2] - Data page header v2
 */

/**
 * @typedef {Object.<string, any>} ThriftObject
 */

/**
 * @typedef {Object} ParquetWriter
 * @property {Writer} writer - Writer used
 * @property {SchemaElement[]} schema - Schema
 * @property {boolean} compressed - Whether data is compressed
 * @property {boolean} statistics - Whether statistics are included
 * @property {KeyValue[]} [kvMetadata] - Key-value metadata
 * @property {RowGroup[]} row_groups - Row groups
 * @property {bigint} num_rows - Number of rows
 * @property {function({columnData: ColumnData[], rowGroupSize?: number}): void} write - Write data
 * @property {function(): void} finish - Finish writing
 */

/**
 * @typedef {Object} ParquetWriteOptions
 * @property {Writer} writer - Writer
 * @property {ColumnData[]} columnData - Column data
 * @property {boolean} [compressed] - Compress data
 * @property {boolean} [statistics] - Include statistics
 * @property {number} [rowGroupSize] - Row group size
 * @property {KeyValue[]} [kvMetadata] - Key-value metadata
 */

export {}