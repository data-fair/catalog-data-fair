/**
 * Tests for the prepare function of the catalog-data-fair plugin
 *
 * The prepare function is responsible for:
 * - Validating the catalog configuration
 * - Securely handling API keys (moving them to secrets)
 * - Testing connectivity with the remote Data Fair instance
 */

import type { DataFairConfig } from '#types'
import type { CatalogPlugin, PrepareContext } from '@data-fair/types-catalogs'
import plugin from '../index.ts'
import type { DataFairCapabilities } from '../lib/capabilities.ts'
import assert from 'assert'
import nock from 'nock'
import { describe, beforeEach, it } from 'node:test'

const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

/** Mock catalog configuration for testing purposes. */
const catalogConfig: DataFairConfig = {
  url: 'https://example.com',
}

describe('catalog-data-fair prepare function', () => {
  beforeEach(() => {
    nock.cleanAll()
  })

  describe('test prepare method', () => {
    it('should prepare successfully with valid configuration', async () => {
      const context: PrepareContext<DataFairConfig, DataFairCapabilities> = {
        catalogConfig,
        secrets: {},
        capabilities: ['import', 'search', 'pagination', 'importConfig', 'thumbnail']
      }

      // Mock the test endpoint call made by prepare
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=1&select=id')
        .reply(200, { count: 0, results: [] })

      const result = await catalogPlugin.prepare(context)

      // The prepare function should complete and return the context
      assert.ok(result, 'Prepare should return a result')
      assert.ok(result.catalogConfig, 'Should return catalog config')
      assert.strictEqual((result.catalogConfig as DataFairConfig).url, 'https://example.com')
    })

    it('should handle prepare with API key', async () => {
      // Setup: Create context with API key in config (simulating user input)
      const context: PrepareContext<DataFairConfig, DataFairCapabilities> = {
        catalogConfig: { ...catalogConfig, apiKey: 'testApiKey' },
        secrets: {},
        capabilities: ['import', 'search', 'pagination', 'importConfig', 'thumbnail']
      }

      // Mock the connectivity test endpoint with API key validation
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=1&select=id')
        .matchHeader('x-apiKey', 'testApiKey')
        .reply(200, { count: 0, results: [] })

      const result = await catalogPlugin.prepare(context)

      // Verify: API key should be moved to secrets and masked in config for security
      assert.ok(result, 'Prepare should return a result')
      assert.ok(result.catalogConfig, 'Should return catalog config')
      assert.ok(result.secrets, 'Should return secrets')
      assert.strictEqual((result.catalogConfig as DataFairConfig).apiKey, '*************************')
      assert.strictEqual((result.secrets as any).apiKey, 'testApiKey')
    })

    it('should handle prepare errors gracefully', async () => {
      const context: PrepareContext<DataFairConfig, DataFairCapabilities> = {
        catalogConfig: { url: 'https://invalid-url.example' },
        secrets: {},
        capabilities: ['import', 'search', 'pagination', 'importConfig', 'thumbnail']
      }

      // Mock a failing prepare endpoint
      nock('https://invalid-url.example')
        .get('/data-fair/api/v1/catalog/datasets?size=1&select=id')
        .reply(404, { error: 'Not Found' })

      await assert.rejects(
        async () => {
          await catalogPlugin.prepare(context)
        },
        /Configuration invalide/i,
        'Should throw configuration error'
      )
    })

    it('should handle missing URL', async () => {
      const context: PrepareContext<DataFairConfig, DataFairCapabilities> = {
        catalogConfig: { url: '' },
        secrets: {},
        capabilities: ['import', 'search', 'pagination', 'importConfig', 'thumbnail']
      }

      await assert.rejects(
        async () => {
          await catalogPlugin.prepare(context)
        },
        /URL du catalogue non définie/i,
        'Should throw error for missing URL'
      )
    })

    it('should handle network errors during prepare', async () => {
      const context: PrepareContext<DataFairConfig, DataFairCapabilities> = {
        catalogConfig,
        secrets: {},
        capabilities: ['import', 'search', 'pagination', 'importConfig', 'thumbnail']
      }

      // Mock network error
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=1&select=id')
        .replyWithError('Network connection failed')

      await assert.rejects(
        async () => {
          await catalogPlugin.prepare(context)
        },
        /Configuration invalide/i,
        'Should throw configuration error for network issues'
      )
    })

    it('should clean up API key from secrets when empty in config', async () => {
      const context: PrepareContext<DataFairConfig, DataFairCapabilities> = {
        catalogConfig: { ...catalogConfig, apiKey: '' },
        secrets: { apiKey: 'oldApiKey' },
        capabilities: ['import', 'search', 'pagination', 'importConfig', 'thumbnail']
      }

      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=1&select=id')
        .reply(200, { count: 0, results: [] })

      const result = await catalogPlugin.prepare(context)

      // API key should be removed from secrets
      assert.ok(result, 'Prepare should return a result')
      assert.ok(result.secrets, 'Should return secrets')
      assert.ok(!(result.secrets as any).apiKey, 'API key should be removed from secrets')
    })
  })
})
