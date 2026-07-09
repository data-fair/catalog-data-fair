/**
 * Tests for the getResource function of the catalog-data-fair plugin
 *
 * Data Fair stores `modified` with a day precision (json schema `format: 'date'`),
 * while `dataUpdatedAt` and `updatedAt` are full `date-time` timestamps. The
 * Resource contract of @data-fair/types-catalogs uses `format: 'date'` too, so the
 * fallback between those fields has to be narrowed down to a day.
 */

import type { DataFairConfig } from '#types'
import type { CatalogPlugin, GetResourceContext, LogFunctions } from '@data-fair/types-catalogs'
import plugin from '../index.ts'
import assert from 'assert'
import fs from 'fs'
import nock from 'nock'
import os from 'os'
import { join } from 'path'
import { describe, beforeEach, afterEach, it } from 'node:test'

const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

/** The `format: 'date'` production of RFC 3339, as validated by ajv-formats. */
const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/

const noopLog: LogFunctions = {
  info: async () => {},
  warning: async () => {},
  error: async () => {},
  step: async () => {},
  task: async () => {},
  progress: async () => {}
}

/** Fetch a resource whose remote dataset carries the given date fields. */
const getResourceWithDates = async (tmpDir: string, dates: Record<string, string>) => {
  nock('https://example.com')
    .get('/data-fair/api/v1/datasets/ds1')
    .reply(200, { id: 'ds1', title: 'A dataset', schema: [], ...dates })
    .get('/data-fair/api/v1/datasets/ds1/lines?format=csv&size=10000')
    .reply(200, 'col\nvalue\n')

  const context: GetResourceContext<DataFairConfig> = {
    catalogConfig: { url: 'https://example.com' },
    secrets: {},
    importConfig: {},
    resourceId: 'ds1',
    tmpDir,
    log: noopLog,
    update: { metadata: true, schema: true }
  }

  return await catalogPlugin.getResource(context)
}

describe('catalog-data-fair getResource function', () => {
  let tmpDir: string

  beforeEach(() => {
    nock.cleanAll()
    tmpDir = fs.mkdtempSync(join(os.tmpdir(), 'catalog-data-fair-test-'))
  })

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  describe('modified metadata', () => {
    it('should keep an already day-precise modified date', async () => {
      const resource = await getResourceWithDates(tmpDir, { modified: '2026-07-09' })
      assert.strictEqual(resource.modified, '2026-07-09')
    })

    it('should narrow dataUpdatedAt down to a day when modified is absent', async () => {
      const resource = await getResourceWithDates(tmpDir, {
        dataUpdatedAt: '2026-07-09T14:25:55.123Z',
        updatedAt: '2026-07-01T08:00:00.000Z'
      })
      assert.match(resource.modified!, DATE_ONLY, 'modified must match json schema format "date"')
      assert.strictEqual(resource.modified, '2026-07-09')
    })

    it('should narrow updatedAt down to a day when modified and dataUpdatedAt are absent', async () => {
      const resource = await getResourceWithDates(tmpDir, { updatedAt: '2026-07-01T08:00:00.000Z' })
      assert.match(resource.modified!, DATE_ONLY, 'modified must match json schema format "date"')
      assert.strictEqual(resource.modified, '2026-07-01')
    })

    it('should leave modified undefined when the dataset carries no date at all', async () => {
      const resource = await getResourceWithDates(tmpDir, {})
      assert.strictEqual(resource.modified, undefined)
    })
  })
})
