import type { DataFairConfig, DataFairDataset } from '#types'
import type { CatalogPlugin, ListResourcesContext } from '@data-fair/types-catalogs'
import plugin from '../index.ts'
import type { DataFairCapabilities } from '../lib/capabilities.ts'
import { getResource } from '../lib/download.ts'
import { logFunctions } from './test-utils.ts'
import assert from 'assert'
import nock from 'nock'
import { describe, beforeEach, it } from 'node:test'
import { tmpdir } from 'os'
import fs from 'fs'

const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

/** Mock catalog configuration for testing purposes. */
const catalogConfig: DataFairConfig = {
  url: 'https://example.com',
}

describe('catalog-data-fair', () => {
  const context: ListResourcesContext<DataFairConfig, DataFairCapabilities> = {
    catalogConfig,
    params: {},
    secrets: {}
  }

  describe('test list method', () => {
    beforeEach(() => {
      nock.cleanAll()
      context.params = {}
    })

    const resources: DataFairDataset[] = [
      {
        id: 'res-1',
        title: 'Resource 1',
        file: {
          size: 1000
        }
      },
      {
        id: 'res-2',
        title: 'Resource 2',
        storage: {
          size: 2000
        }
      }]

    it('should list resources', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=2&page=10').reply(200, { count: 10, results: resources })

      const res = await catalogPlugin.listResources({
        catalogConfig,
        secrets: {},
        params: { page: 10, size: 2 }
      })

      assert.strictEqual(res.count, 10, 'Expected 2 items in the root folder')
      assert.strictEqual(res.results.length, 2)
      assert.strictEqual(res.results[0].type, 'resource', 'Expected Resource')
      assert.strictEqual(res.path.length, 0, 'Expected no path')
      assert.strictEqual(JSON.stringify(res.results[0]), JSON.stringify({ id: 'res-1', title: 'Resource 1', format: 'csv', size: 1000, type: 'resource' }))
      assert.strictEqual(JSON.stringify(res.results[1]), JSON.stringify({ id: 'res-2', title: 'Resource 2', format: 'csv', size: 2000, type: 'resource' }))
    })

    it('should fail for resource not found', async () => {
      nock('https://testErr')
        .get('/data-fair/api/v1/catalog/datasets').reply(404, { error: 'Not Found' })

      await assert.rejects(
        async () => {
          await catalogPlugin.listResources({
            catalogConfig: { url: 'https://testErr' },
            secrets: {},
            params: {}
          } as ListResourcesContext<DataFairConfig, DataFairCapabilities>)
        },
        /Erreur lors de la récuperation de la resource Data Fair|Not Found/i,
        'Should throw an error for non-existent resource'
      )
    })
  })

  describe('test the download function', () => {
    const tmpDir = tmpdir()
    const resourceId = 'my-test-resource'

    const downloadContext = {
      catalogConfig,
      resourceId,
      importConfig: {
        fields: [],
        filters: [],
      },
      tmpDir,
      log: logFunctions
    }

    it('should download full resource file when size > 0 and no filters', async () => {
      const metaUrl = `/data-fair/api/v1/datasets/${resourceId}`
      const fullFileUrl = `/data-fair/api/v1/datasets/${resourceId}/full`

      // Mock metadata request
      nock(catalogConfig.url)
        .get(metaUrl)
        .reply(200, {
          id: resourceId,
          title: 'Test Resource',
          description: 'A simple CSV resource',
          frequency: 'monthly',
          image: null,
          keywords: ['test'],
          file: {
            size: 1234
          },
          schema: {},
        })

      // Mock file download stream
      const csvData = 'col1,col2\nval1,val2\nval3,val4\n'
      nock(catalogConfig.url)
        .get(fullFileUrl)
        .reply(200, csvData, { 'Content-Type': 'text/csv' })

      const resource = await getResource(downloadContext as any)

      // Check file exists and content matches
      assert.ok(resource?.filePath, 'File path should be set in resource')
      assert.ok(fs.existsSync(resource.filePath), 'Downloaded file should exist')
      const content = fs.readFileSync(resource.filePath, 'utf8')
      assert.match(content, /val1,val2/, 'CSV should contain expected data')
    })

    it('should download filtered resource as lines with select and filters', async () => {
      const resourceId = 'filtered-resource'
      const contextWithFilters = {
        catalogConfig,
        resourceId,
        importConfig: {
          fields: [{ key: 'field1' }, { key: 'field2' }],
          filters: [{ field: { key: 'year' }, type: 'gte', value: '2020' }]
        },
        tmpDir,
        log: logFunctions
      }

      const metaUrl = `/data-fair/api/v1/datasets/${resourceId}`
      const linesUrl = `/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=3000&select=field1,field2&year_gte=2020`

      // Mock metadata
      nock(catalogConfig.url)
        .get(metaUrl)
        .reply(200, {
          id: resourceId,
          title: 'Filtered Resource',
          description: '',
          frequency: 'monthly',
          image: null,
          keywords: [],
          schema: {},
        })

      // Mock first page of filtered data
      const csvLines = 'field1,field2\n2021,abc\n2022,def\n'
      nock(catalogConfig.url)
        .get(linesUrl)
        .reply(200, csvLines, {
          'Content-Type': 'text/csv',
          Link: ''
        })

      const resource = await getResource(contextWithFilters as any)

      assert.ok(resource?.filePath, 'File path should be returned')
      assert.ok(fs.existsSync(resource.filePath), 'File should be downloaded')
      const content = fs.readFileSync(resource.filePath, 'utf8')
      assert.match(content, /field1,field2/, 'Header should exist')
      assert.match(content, /2022,def/, 'Expected data row should be present')
    })
  })
})
