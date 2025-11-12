import type CatalogPlugin from '@data-fair/types-catalogs'
import { importConfigSchema, configSchema, assertConfigValid, type DataFairConfig } from '#types'
import { type DataFairCapabilities, capabilities } from './lib/capabilities.ts'

// Since the plugin is very frequently imported, each function is imported on demand,
// instead of loading the entire plugin.
// This file should not contain any code, but only constants and dynamic imports of functions.

const plugin: CatalogPlugin<DataFairConfig, DataFairCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
  },

  async list (context) {
    const { listResources } = await import('./lib/imports.ts')
    return listResources(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/download.ts')
    return getResource(context)
  },

  metadata: {
    title: 'Catalog Data Fair',
    description: 'Data Fair plugin for Data Fair Catalog',
    thumbnailPath: './lib/resources/thumbnail.svg',
    capabilities
  },

  importConfigSchema,
  configSchema,
  assertConfigValid
}
export default plugin
