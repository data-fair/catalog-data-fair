import type { DataFairConfig, DataFairDataset, ImportConfig } from '#types'
import type { CatalogPlugin, GetResourceContext, ListResourcesContext } from '@data-fair/types-catalogs'
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

  /**
   * Test suite for the listResources functionality
   *
   * The listResources function is responsible for:
   * - Fetching available datasets from a Data Fair instance
   * - Supporting pagination through page/size parameters
   * - Handling search queries via the 'q' parameter
   * - Transforming Data Fair datasets into catalog resource format
   * - Managing authentication via API keys
   */
  describe('test list method', () => {
    beforeEach(() => {
      nock.cleanAll()
      context.params = {}
    })

    const resources: DataFairDataset[] = [
      {
        id: 'res-1',
        title: 'Resource 1',
        page: 'https://example.com/resource-1'
      },
      {
        id: 'res-2',
        title: 'Resource 2',
        storage: {
          size: 2000,
          dataFiles: [
            { key: 'raw', size: 500 },
            { key: 'full', size: 1500 }
          ]
        }
      }]

    /**
     * Test basic resource listing functionality
     *
     * Verifies that the plugin can successfully fetch and transform
     * a list of datasets from the Data Fair API with pagination.
     */
    it('should list resources', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=2&page=10')
        .reply(200, { count: 10, results: resources })

      const res = await catalogPlugin.listResources({
        catalogConfig,
        secrets: {},
        params: { page: 10, size: 2 }
      })

      assert.strictEqual(res.count, 10, 'Expected 2 items in the root folder')
      assert.strictEqual(res.results.length, 2)
      assert.strictEqual(res.results[0].type, 'resource', 'Expected Resource')
      assert.strictEqual(res.path.length, 0, 'Expected no path')
      assert.deepEqual(res.results[0], { id: 'res-1', title: 'Resource 1', format: 'csv', size: undefined, type: 'resource', origin: 'https://example.com/resource-1' })
      assert.deepEqual(res.results[1], { id: 'res-2', title: 'Resource 2', format: 'csv', size: 1500, type: 'resource', origin: undefined })
    })

    /**
     * Test resource listing with API key authentication
     *
     * Ensures that API keys stored in secrets are properly used
     * for authenticating requests to the Data Fair API.
     */
    it('should list with api key', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?size=2&page=10')
        .matchHeader('x-apiKey', 'testApiKey')
        .reply(200, { count: 10, results: resources })

      const res = await catalogPlugin.listResources({
        catalogConfig: { ...catalogConfig, apiKey: '********' },
        secrets: { apiKey: 'testApiKey' },
        params: { page: 10, size: 2 }
      })
      assert.strictEqual(res.count, 10, 'Expected 2 items in the root folder')
      assert.strictEqual(res.results.length, 2)
      assert.strictEqual(res.results[0].type, 'resource', 'Expected Resource')
      assert.strictEqual(res.path.length, 0, 'Expected no path')
      assert.deepEqual(res.results[0], { id: 'res-1', title: 'Resource 1', format: 'csv', size: undefined, type: 'resource', origin: 'https://example.com/resource-1' })
      assert.deepEqual(res.results[1], { id: 'res-2', title: 'Resource 2', format: 'csv', size: 1500, type: 'resource', origin: undefined })
    })

    /**
     * Test error handling for non-existent resources
     *
     * Verifies that the plugin properly handles and reports errors
     * when the Data Fair API returns 404 or other error responses.
     */
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

    /**
     * Test search functionality with query parameters
     *
     * Validates that search queries are properly passed to the Data Fair API
     * and that filtered results are correctly returned.
     */
    it('should handle search query parameter', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets?q=test&size=10')
        .reply(200, { count: 1, results: [resources[0]] })

      const res = await catalogPlugin.listResources({
        catalogConfig,
        secrets: {},
        params: { q: 'test', size: 10 }
      })

      assert.strictEqual(res.count, 1)
      assert.strictEqual(res.results.length, 1)
      assert.strictEqual(res.results[0].title, 'Resource 1')
    })

    /**
     * Test handling of empty result sets
     *
     * Ensures the plugin gracefully handles cases where the Data Fair
     * instance returns no datasets (empty catalog).
     */
    it('should handle empty results', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets')
        .reply(200, { count: 0, results: [] })

      const res = await catalogPlugin.listResources({
        catalogConfig,
        secrets: {},
        params: {}
      })

      assert.strictEqual(res.count, 0)
      assert.strictEqual(res.results.length, 0)
      assert.strictEqual(res.path.length, 0)
    })

    /**
     * Test network error handling
     *
     * Verifies that network-level errors (connection failures, timeouts)
     * are properly caught and converted to user-friendly error messages.
     */
    it('should handle network errors', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets')
        .replyWithError('Network error')

      await assert.rejects(
        async () => {
          await catalogPlugin.listResources({
            catalogConfig,
            secrets: {},
            params: {}
          })
        },
        /Erreur lors de la récuperation de la resource Data Fair/i,
        'Should throw an error for network issues'
      )
    })

    /**
     * Test HTTP 500 error handling
     *
     * Ensures that server errors from the Data Fair API are properly
     * handled and reported to the user.
     */
    it('should handle HTTP errors (500)', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets')
        .reply(500, { error: 'Internal Server Error' })

      await assert.rejects(
        async () => {
          await catalogPlugin.listResources({
            catalogConfig,
            secrets: {},
            params: {}
          })
        },
        /HTTP error|Erreur lors de la récuperation de la resource Data Fair/i,
        'Should throw an error for HTTP 500'
      )
    })

    /**
     * Test handling of resources without size information
     *
     * Some Data Fair datasets may not have storage size information.
     * This test ensures the plugin handles such cases gracefully.
     */
    it('should handle resources without size information', async () => {
      const noSizeResources = [{
        id: 'no-size-res',
        title: 'No Size Resource'
      },
      {
        id: 'no-size-res-2',
        title: 'No Size Resource 2',
        storage: {
          dataFiles: []
        }
      }]

      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets')
        .reply(200, { count: 2, results: noSizeResources })

      const res = await catalogPlugin.listResources({
        catalogConfig,
        secrets: {},
        params: {}
      })

      assert.strictEqual((res.results[0] as any).size, undefined, 'Size should be undefined when not available')
      assert.strictEqual((res.results[1] as any).size, undefined, 'Size should be undefined when not available')
    })
  })

  /**
   * Test suite for the download functionality (getResource)
   *
   * The getResource function is responsible for:
   * - Downloading dataset files from Data Fair instances
   * - Handling different download strategies (full files vs. filtered lines)
   * - Processing import configurations (field selection, filters)
   * - Managing metadata extraction and schema processing
   * - Supporting pagination for large datasets
   * - Handling authentication and error scenarios
   */
  describe('test the download function', () => {
    const tmpDir = tmpdir()

    /**
     * Test downloading full dataset files
     *
     * When no filters are applied and a 'full' file is available,
     * the plugin should download the complete dataset file.
     */
    it('should download full resource file when /full is available and no filters', async () => {
      const resourceId = 'my-test-resource'

      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

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
          keywords: ['test'],
          file: {
            size: 1234
          },
          schema: [],
        } as DataFairDataset)

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

    /**
     * Test download with API key authentication
     *
     * Ensures that API keys from secrets are properly included
     * in both metadata and file download requests.
     */
    it('should call with apiKey in secrets', async () => {
      const resourceId = 'my-test-resource-with-apiKey'

      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig: { ...catalogConfig, apiKey: '********' },
        resourceId,
        secrets: { apiKey: 'testApiKey' },
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

      const metaUrl = `/data-fair/api/v1/datasets/${resourceId}`
      const linesUrl = `/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000`
      // Mock metadata request
      nock(catalogConfig.url)
        .get(metaUrl)
        .matchHeader('x-apiKey', 'testApiKey')
        .reply(200, {
          id: resourceId,
          title: 'Test Resource',
          description: 'A simple CSV resource',
        })
      // Mock file download stream
      const csvData = 'col1,col2\nval1,val2\nval3,val4\n'
      nock(catalogConfig.url)
        .get(linesUrl)
        .matchHeader('x-apiKey', 'testApiKey')
        .reply(200, csvData, { 'Content-Type': 'text/csv' })

      const resource = await getResource(downloadContext)
      // Check file exists and content matches
      assert.ok(resource?.filePath, 'File path should be set in resource')
      assert.ok(fs.existsSync(resource.filePath), 'Downloaded file should exist')
      const content = fs.readFileSync(resource.filePath, 'utf8')
      assert.match(content, /val1,val2/, 'CSV should contain expected data')
    })

    /**
     * Test filtered download with field selection and filters
     *
     * When import configuration includes field selection or filters,
     * the plugin should use the /lines endpoint with appropriate parameters
     * instead of downloading the full file.
     */
    it('should download filtered resource as lines with select and filters', async () => {
      const resourceId = 'filtered-resource'
      const contextWithFilters: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {
          fields: [{ key: 'field1' }, { key: 'field2' }],
          filters: [{ field: { key: 'year' }, type: 'gte', value: '2020' }]
        },
        tmpDir,
        log: logFunctions
      }

      const metaUrl = `/data-fair/api/v1/datasets/${resourceId}`
      const linesUrl = `/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000&select=field1,field2&year_gte=2020`
      // Mock metadata
      nock(catalogConfig.url)
        .get(metaUrl)
        .reply(200, {
          id: resourceId,
          title: 'Filtered Resource',
          description: '',
          frequency: 'monthly',
        } as DataFairDataset)

      // Mock first page of filtered data
      const csvLines = 'field1,field2\n2021,abc\n2022,def\n'
      nock(catalogConfig.url)
        .get(linesUrl)
        .reply(200, csvLines, { 'Content-Type': 'text/csv' })

      const resource = await getResource(contextWithFilters as any)

      assert.ok(resource?.filePath, 'File path should be returned')
      assert.ok(fs.existsSync(resource.filePath), 'File should be downloaded')
      const content = fs.readFileSync(resource.filePath, 'utf8')
      console.log('Downloaded content:', content)
      assert.match(content, /field1,field2/, 'Header should exist')
      assert.match(content, /2022,def/, 'Expected data row should be present')
    })

    /**
     * Test error handling during metadata fetch
     *
     * Verifies proper error handling when the dataset metadata
     * cannot be retrieved from the Data Fair API.
     */
    it('should handle metadata fetch errors', async () => {
      const resourceId = 'error-resource'
      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}`)
        .reply(404, { error: 'Dataset not found' })

      await assert.rejects(
        async () => await getResource(downloadContext as any),
        /Erreur lors de la récuperation de la resource DataFair/i,
        'Should throw error when metadata fetch fails'
      )
    })

    /**
     * Test error handling during file download
     *
     * Ensures that errors during the actual file download process
     * are properly caught and reported.
     */
    it('should handle file download errors', async () => {
      const resourceId = 'download-error-resource'
      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

      // Mock successful metadata request
      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}`)
        .reply(200, {
          id: resourceId,
          title: 'Test Resource',
          file: { size: 1000 }
        })

      // Mock failed file download
      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}/full`)
        .reply(500, 'Server error')

      await assert.rejects(
        async () => await getResource(downloadContext as any),
        /Erreur pendant le téléchargement du fichier/i,
        'Should throw error when file download fails'
      )
    })

    it('should handle resources with license information', async () => {
      const resourceId = 'licensed-resource'
      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}`)
        .reply(200, {
          id: resourceId,
          title: 'Licensed Resource',
          description: 'A resource with license',
          license: {
            title: 'MIT License',
            href: 'https://opensource.org/licenses/MIT'
          },
          file: { size: 500 }
        })

      const csvData = 'col1,col2\nval1,val2\n'
      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}/full`)
        .reply(200, csvData, { 'Content-Type': 'text/csv' })

      const resource = await getResource(downloadContext as any)

      assert.ok(resource, 'Resource should be returned')
      assert.ok(resource!.license, 'License should be present')
      assert.strictEqual(resource!.license?.title, 'MIT License')
      assert.strictEqual(resource!.license?.href, 'https://opensource.org/licenses/MIT')
    })

    it('should handle pagination in line download', async () => {
      const resourceId = 'paginated-resource'
      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

      // Mock metadata
      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}`)
        .reply(200, {
          id: resourceId,
          title: 'Paginated Resource'
        })

      // Mock first page
      const firstPageData = 'col1,col2\nval1,val2\n'
      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000`)
        .reply(200, firstPageData, {
          'Content-Type': 'text/csv',
          link: '<https://example.com/data-fair/api/v1/datasets/paginated-resource/lines?format=csv&size=5000&after=12>; rel=next'
        })

      // Mock second page
      const secondPageData = 'col1,col2\nval3,val4\n'
      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000&after=12`)
        .reply(200, secondPageData, { 'Content-Type': 'text/csv' })

      const resource = await getResource(downloadContext as any)

      assert.ok(resource, 'Resource should be returned')
      assert.ok(resource!.filePath, 'File path should be set')
      assert.ok(fs.existsSync(resource!.filePath), 'File should exist')
      const content = fs.readFileSync(resource!.filePath, 'utf8')
      console.log('Downloaded content:', content)
      assert.match(content, /val1,val2/, 'First page data should be present')
      assert.match(content, /val3,val4/, 'Second page data should be present')
    })

    it('should handle complex filters in import config', async () => {
      const resourceId = 'complex-filters-resource'
      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {
          filters: [
            { field: { key: 'status' }, type: 'in', values: ['active', 'pending'] },
            { field: { key: 'category' }, type: 'nin', values: ['archived'] },
            { field: { key: 'name' }, type: 'starts', value: 'test' }
          ]
        } as ImportConfig,
        tmpDir,
        log: logFunctions
      }

      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}`)
        .reply(200, {
          id: resourceId,
          title: 'Filtered Resource'
        })

      const expectedUrl = `/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000&status_in="active","pending"&category_nin="archived"&name_starts=test`
      nock(catalogConfig.url)
        .get(expectedUrl)
        .reply(200, 'name,status,category\ntest1,active,main\n', { 'Content-Type': 'text/csv' })

      const resource = await getResource(downloadContext as any)

      assert.ok(resource, 'Resource should be returned')
      assert.ok(resource!.filePath, 'File should be downloaded with complex filters')
      const content = fs.readFileSync(resource!.filePath, 'utf8')
      console.log('Downloaded content:', content)
      assert.match(content, /test1,active,main/, 'Filtered data should be present')
    })

    it('should handle resources with schema extensions (remove the extension)', async () => {
      const resourceId = 'schema-resource'
      const downloadContext: GetResourceContext<DataFairConfig> = {
        catalogConfig,
        resourceId,
        secrets: {},
        importConfig: {},
        tmpDir,
        log: logFunctions
      }

      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}`)
        .reply(200, {
          id: resourceId,
          title: 'Schema Resource',
          schema: [
            {
              key: 'field1',
              type: 'string',
              'x-extension': 'geo-point'
            },
            {
              key: 'field2',
              type: 'string'
            }
          ]
        })

      nock(catalogConfig.url)
        .get(`/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000`)
        .reply(200, 'field1\n"45.123,2.456"\n', { 'Content-Type': 'text/csv' })

      const resource = await getResource(downloadContext as any)

      assert.ok(resource, 'Resource should be returned')
      assert.ok(resource!.schema, 'Schema should be present')
      assert.strictEqual(resource!.schema?.[0]?.key, 'field1')
      assert.strictEqual(resource.schema?.[0]?.['x-extension'], undefined, 'Extension should be removed')
    })
  })

  describe('test plugin capabilities and metadata', () => {
    it('should have correct capabilities', () => {
      assert.ok(catalogPlugin.metadata?.capabilities, 'Plugin should have capabilities')
      const capabilities = catalogPlugin.metadata.capabilities
      assert.ok(capabilities.includes('import'), 'Should support import')
      assert.ok(capabilities.includes('search'), 'Should support search')
      assert.ok(capabilities.includes('pagination'), 'Should support pagination')
      assert.ok(capabilities.includes('importConfig'), 'Should support importConfig')
      assert.ok(capabilities.includes('thumbnail'), 'Should support thumbnail')
    })

    it('should have correct metadata', () => {
      assert.strictEqual(catalogPlugin.metadata?.title, 'Catalog Data Fair')
      assert.strictEqual(catalogPlugin.metadata?.description, 'Data Fair plugin for Data Fair Catalog')
      assert.ok(catalogPlugin.metadata?.thumbnailPath, 'Should have thumbnail path')
    })

    it('should have schema definitions', () => {
      assert.ok(catalogPlugin.configSchema, 'Should have config schema')
      assert.ok(catalogPlugin.importConfigSchema, 'Should have import config schema')
      assert.ok(catalogPlugin.assertConfigValid, 'Should have config validation function')
    })
  })

  describe('test edge cases and error handling', () => {
    it('should handle malformed JSON responses', async () => {
      nock('https://example.com')
        .get('/data-fair/api/v1/catalog/datasets')
        .reply(200, 'invalid json')

      await assert.rejects(
        async () => {
          await catalogPlugin.listResources({
            catalogConfig,
            secrets: {},
            params: {}
          })
        },
        /Erreur lors de la récuperation de la resource Data Fair/i,
        'Should handle malformed JSON'
      )
    })
  })
})
