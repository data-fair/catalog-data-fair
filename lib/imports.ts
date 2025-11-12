import type { DataFairCatalog, DataFairDataset, DataFairConfig } from '#types'
import axios from '@data-fair/lib-node/axios.js'
import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { DataFairCapabilities } from './capabilities.ts'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

/**
 * @param dataFairDataset the dataset to transform
 * @returns an object containing the count of resources, the transformed resources, and an empty path array
 */
const prepareCatalog = (dataFairDatasets: DataFairDataset[]): ResourceList => {
  const catalog: ResourceList = []

  for (const dataFairDataset of dataFairDatasets) {
    let size: number | undefined
    if (dataFairDataset.storage?.dataFiles && dataFairDataset.storage.dataFiles.length > 0) {
      const lastFile = dataFairDataset.storage.dataFiles[dataFairDataset.storage.dataFiles.length - 1]
      size = lastFile.size
    }
    catalog.push({
      id: dataFairDataset.id,
      title: dataFairDataset.title,
      format: 'csv',
      size,
      type: 'resource',
      origin: dataFairDataset.page
    } as ResourceList[number])
  }
  return catalog
}

/**
 * Returns the catalog [list of dataset] from a Data Fair service
 * @param config the Data Fair configuration
 * @returns the list of Resources available on this catalog
 */
export const listResources = async (config: ListContext<DataFairConfig, DataFairCapabilities>): ReturnType<CatalogPlugin<DataFairConfig>['list']> => {
  const dataFairParams: Record<string, any> = { sort: 'title' }
  if (config.params?.q) dataFairParams.q = config.params.q
  if (config.params?.size) dataFairParams.size = config.params.size
  if (config.params?.page) dataFairParams.page = config.params.page

  let data: DataFairCatalog
  const url = `${config.catalogConfig.url}/data-fair/api/v1/catalog/datasets`
  const headers = config.secrets.apiKey ? { 'x-apiKey': config.secrets.apiKey } : undefined
  try {
    const res = (await axios.get(url, { params: dataFairParams, headers }))
    if (res.status !== 200 || typeof res.data !== 'object') {
      throw new Error(`HTTP error : ${res.status}, ${res.data}`)
    }
    data = res.data
  } catch (e) {
    console.error(`Error fetching datasets from ${url} : ${e}`)
    throw new Error(`Erreur lors de la récuperation de la resource Data Fair (${e instanceof Error ? e.message : ''})`)
  }

  const catalog = prepareCatalog(data.results)
  return {
    count: data.count,
    results: catalog,
    path: []
  }
}
