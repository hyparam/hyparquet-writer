import { getMaxDefinitionLevel } from './schema.js'

/**
 * Encode nested list values into repetition and definition levels.
 *
 * @import {SchemaElement} from 'hyparquet'
 * @import {PageData} from '../src/types.js'
 * @param {SchemaElement[]} schemaPath schema elements from root to leaf
 * @param {any[]} rows column data for the current row group
 * @returns {PageData} encoded list values
 */
export function encodeListValues(schemaPath, rows) {
  if (schemaPath.length < 2) throw new Error('parquet list schema path must include column')
  /** @type {any[]} */
  const values = []
  /** @type {number[]} */
  const definitionLevels = []
  /** @type {number[]} */
  const repetitionLevels = []

  // Track repetition depth prior to each level
  const repLevelPrior = new Array(schemaPath.length)
  let repeatedCount = 0
  for (let i = 0; i < schemaPath.length; i++) {
    repLevelPrior[i] = repeatedCount
    if (schemaPath[i].repetition_type === 'REPEATED') repeatedCount++
  }

  const leafIndex = schemaPath.length - 1
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)

  for (let row = 0; row < rows.length; row++) {
    visit(1, rows[row], 0, 0, false)
  }

  const numNulls = definitionLevels.reduce(
    (count, def) => def === maxDefinitionLevel ? count : count + 1,
    0
  )

  return { values, definitionLevels, repetitionLevels, numNulls }

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
    const isLeaf = depth === leafIndex

    if (isLeaf) {
      if (value === null || value === undefined) {
        if (repetition === 'REQUIRED' && !allowNull) {
          throw new Error('parquet required value is undefined')
        }
        definitionLevels.push(defLevel)
        repetitionLevels.push(repLevel)
        values.push(null)
      } else {
        const finalDef = repetition === 'REQUIRED' ? defLevel : defLevel + 1
        definitionLevels.push(finalDef)
        repetitionLevels.push(repLevel)
        values.push(value)
      }
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
      for (let i = 0; i < value.length; i++) {
        const childRep = i === 0 ? repLevel : repLevelPrior[depth] + 1
        visit(depth + 1, value[i], defLevel + 1, childRep, false)
      }
      return
    }

    if (repetition === 'OPTIONAL') {
      if (value === null || value === undefined) {
        visit(depth + 1, undefined, defLevel, repLevel, true)
      } else {
        visit(depth + 1, value, defLevel + 1, repLevel, false)
      }
      return
    }

    // REQUIRED
    if (value === null || value === undefined) {
      if (!allowNull) throw new Error('parquet required value is undefined')
      visit(depth + 1, undefined, defLevel, repLevel, true)
    } else {
      visit(depth + 1, value, defLevel, repLevel, false)
    }
  }
}
