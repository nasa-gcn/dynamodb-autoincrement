import jestDynamodb from '@shelf/jest-dynamodb/jest-preset.js'

/** @type {import('jest').Config} */
export default {
  ...jestDynamodb,
  preset: 'ts-jest',
  collectCoverage: true,
  coverageReporters: ['text', 'cobertura'],
}
