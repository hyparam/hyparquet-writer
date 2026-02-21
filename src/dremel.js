import { getMaxDefinitionLevel, isListLike, isMapLike } from 'hyparquet/src/schema.js'

/**
 * @import {DecodedArray, SchemaElement, SchemaTree} from 'hyparquet'
 * @import {PageData} from '../src/types.js'
 */

/**
 * Encode column values into repetition and definition levels following the
 * Dremel algorithm. Returns page data for one subcolumn (leaf node in the schema).
 *
 * @param {SchemaTree[]} treePath schema tree nodes from root to leaf
 * @param {DecodedArray} rows top-level column data
 * @returns {PageData}
 */
export function encodeNestedValues(treePath, rows) {
  const schemaPath = treePath.map(n => n.element)
  if (treePath.length < 2) throw new Error('parquet schema path must include column')

  /** @type {number[]} */
  const definitionLevels = []
  /** @type {number[]} */
  const repetitionLevels = []
  const maxDefinitionLevel = getMaxDefinitionLevel(treePath)

  // Flat required columns don't need Dremel encoding
  if (treePath.length === 2 && maxDefinitionLevel === 0) {
    return { values: rows, definitionLevels, repetitionLevels, maxDefinitionLevel }
  }

  // Track repetition depth prior to each level
  const repLevelPrior = new Array(treePath.length)
  let repeatedCount = 0
  for (let i = 0; i < treePath.length; i++) {
    repLevelPrior[i] = repeatedCount
    if (schemaPath[i].repetition_type === 'REPEATED') repeatedCount++
  }

  /** @type {any[]} */
  const values = []

  for (const row of rows) {
    visit(1, row, 0, 0, false)
  }

  return { values, definitionLevels, repetitionLevels, maxDefinitionLevel }

  /**
   * Recursively walk the schema path, emitting definition/repetition pairs.
   *
   * @param {number} depth index into schemaPath
   * @param {any} value value at the current depth
   * @param {number} defLevel definition level accumulated so far
   * @param {number} repLevel repetition level for the next emitted slot
   * @param {boolean} allowNull whether the current value is allowed to be null
   */
  function visit(depth, value, defLevel, repLevel, allowNull) {
    const element = schemaPath[depth]
    const repetition = element.repetition_type || 'REQUIRED'

    // Leaf node
    if (depth === treePath.length - 1) {
      if (value === null || value === undefined) {
        if (repetition === 'REQUIRED' && !allowNull) {
          throw new Error('parquet required value is undefined')
        }
        definitionLevels.push(defLevel)
      } else {
        definitionLevels.push(repetition === 'REQUIRED' ? defLevel : defLevel + 1)
      }
      repetitionLevels.push(repLevel)
      values.push(value)
      return
    }

    if (repetition === 'REPEATED') {
      if (value === null || value === undefined) {
        if (!allowNull) throw new Error('parquet required value is undefined')
        visit(depth + 1, undefined, defLevel, repLevel, true)
        return
      }
      if (!Array.isArray(value)) {
        throw new Error(`parquet repeated field ${element.name} must be an array`)
      }
      if (!value.length) {
        visit(depth + 1, undefined, defLevel, repLevel, true)
        return
      }
      // For MAP key_value entries, extract the child field (key or value) from each entry
      const isMapEntry = isMapLike(treePath[depth - 1])
      const childElement = schemaPath[depth + 1]
      for (let i = 0; i < value.length; i++) {
        let childValue = value[i]
        if (isMapEntry && childValue && typeof childValue === 'object' && childElement) {
          childValue = childValue[childElement.name]
        }
        const childRep = i === 0 ? repLevel : repLevelPrior[depth] + 1
        visit(depth + 1, childValue, defLevel + 1, childRep, false)
      }
      return
    }

    if (repetition === 'OPTIONAL') {
      if (value === null || value === undefined) {
        visit(depth + 1, undefined, defLevel, repLevel, true)
      } else {
        const childValue = getChildValue(depth, value)
        const childIsNull = childValue === null || childValue === undefined
        // Increment def level if: (1) this is a struct (contributes to def even if child is null),
        // or (2) the child value exists. LIST/MAP wrappers don't increment def level themselves.
        const isLogicalContainer = isListLike(treePath[depth]) || isMapLike(treePath[depth])
        const isStruct = element.num_children && !element.type && !isLogicalContainer
        const nextDef = isStruct || !childIsNull ? defLevel + 1 : defLevel
        visit(depth + 1, childValue, nextDef, repLevel, childIsNull)
      }
      return
    }

    // REQUIRED
    if (value === null || value === undefined) {
      if (!allowNull) throw new Error('parquet required value is undefined')
      visit(depth + 1, undefined, defLevel, repLevel, true)
    } else {
      visit(depth + 1, getChildValue(depth, value), defLevel, repLevel, false)
    }
  }

  /**
   * Select the child value for the next schema element in the path.
   * Normalizes maps to {key, value} entries.
   *
   * @param {number} depth current schema depth
   * @param {any} currentValue current value at this depth
   * @returns {any}
   */
  function getChildValue(depth, currentValue) {
    if (currentValue === null || currentValue === undefined) return undefined
    const child = schemaPath[depth + 1]
    if (!child) return undefined

    // LIST and MAP wrappers
    if (isListLike(treePath[depth])) return currentValue
    if (isMapLike(treePath[depth])) {
      return normalizeMap(currentValue, schemaPath[depth])
    }

    if (typeof currentValue === 'object' && !Array.isArray(currentValue)) {
      return currentValue[child.name]
    }

    throw new Error(`parquet expected struct, got ${currentValue}`)
  }

}

/**
 * Normalize a map value to an array of {key, value} entries.
 * Accepts Map, plain object, array of [k, v] pairs, or array of {key, value}.
 *
 * @param {any} value
 * @param {SchemaElement} element
 * @returns {{key: any, value: any}[]}
 */
function normalizeMap(value, element) {
  if (value instanceof Map) {
    return Array.from(value.entries(), ([k, v]) => ({ key: k, value: v }))
  }
  if (Array.isArray(value)) {
    return value.map(entry => {
      if (entry && typeof entry === 'object' && 'key' in entry && 'value' in entry) {
        return entry
      }
      if (Array.isArray(entry) && entry.length === 2) {
        return { key: entry[0], value: entry[1] }
      }
      throw new Error('parquet map entry must provide key and value')
    })
  }
  if (typeof value === 'object') {
    return Object.entries(value).map(([k, v]) => ({ key: k, value: v }))
  }
  throw new Error(`parquet map field ${element.name} must be Map, array, or object`)
}
