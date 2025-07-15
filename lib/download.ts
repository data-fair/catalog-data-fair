import type { DataFairConfig, DataFairDataset, ImportConfig } from '#types'
import type { CatalogPlugin, GetResourceContext, Resource } from '@data-fair/types-catalogs'
import axios from '@data-fair/lib-node/axios.js'
import * as fs from 'fs'
import { join } from 'path'
import { Transform } from 'stream'

/**
 * Retrieves a resource by first fetching its metadata and then downloading the actual resource.
 * The downloaded file path is added to the dataset metadata before returning.
 *
 * @param context - The context containing configuration and parameters required to fetch and download the resource.
 * @returns A promise that resolves to the dataset metadata with the downloaded file path included.
 */
export const getResource = async (context: GetResourceContext<DataFairConfig>): ReturnType<CatalogPlugin['getResource']> => {
  context.log.step('Import de la ressource')

  const resource = await getMetaData(context)
  resource.filePath = await downloadResource(context, resource)

  return resource
}

/**
 * Returns the DataFair Resource with all its metadatas
 * @param catalogConfig the DataFair configuration [ex: { url: 'https://example.com' }]
 * @param resourceId the dataset Id to fetch fields from
 * @returns the Resource corresponding to the id by this configuration
 */
const getMetaData = async ({ catalogConfig, resourceId, log }: GetResourceContext<DataFairConfig>): Promise<Resource> => {
  let dataset: DataFairDataset
  try {
    const url = `${catalogConfig.url}/data-fair/api/v1/datasets/${resourceId}`
    const res = (await axios.get(url))
    if (res.status !== 200) {
      throw new Error(`HTTP error : ${res.status}, ${res.data}`)
    }
    dataset = res.data
    log.info('Import des métadonnées de la ressource', { url })
  } catch (e) {
    console.error('Error while fetching metadatas', e)
    throw new Error(`Erreur lors de la récuperation de la resource DataFair. ${e instanceof Error ? e.message : e}`)
  }

  const resource: Resource = {
    id: resourceId,
    title: dataset.title,
    description: dataset.description,
    format: 'csv',
    origin: `${catalogConfig.url}/datasets/${resourceId}`,
    frequency: dataset.frequency,
    image: dataset.image,
    keywords: dataset.keywords,
    size: dataset.file?.size,
    schema: dataset.schema,
    filePath: '',
  }

  if (dataset.license) {
    resource.license = {
      title: dataset.license.title ?? '',
      href: dataset.license.href ?? '',
    }
  }
  return resource
}

/**
 * Download a specified from a Data Fair service.
 * If the resource has a distant file and no import configuration will download the distant file, otherwise the data will be fetch by set of rows.
 * @param context - the download context, contains the download configuration, the resource Id
 * @param res - the metadatas about the resource.
 * @returns A promise resolving to the file path of the downloaded CSV.
 */
const downloadResource = async (context: GetResourceContext<DataFairConfig>, res: Resource): Promise<string> => {
  const filePath = join(context.tmpDir, `${context.resourceId}.csv`)
  try {
    if (res.size && res.size > 0 && context.importConfig.fields?.length === 0 && context.importConfig.filters?.length === 0) {
      await downloadResourceFile(filePath, context)
    } else {
      await downloadResourceLines(filePath, context)
    }
    return filePath
  } catch (error) {
    console.error('Error while downloading the file', error)
    context.log.error(`Erreur pendant le téléchargement du fichier : ${error instanceof Error ? error.message : error}`)
    throw new Error(`Erreur pendant le téléchargement du fichier: ${error instanceof Error ? error.message : String(error)}`)
  }
}

/**
 * Downloads the rows of a dataset thanks to its full file and saves them as a CSV file in a given file path. The configuration  of the importConfig is not applicable.
 * @param filePath - The path to the temporary file where the CSV will be saved.
 * @param catalogConfig - The DataFair configuration object.
 * @param resourceId - The Id of the dataset to download.
 * @param log - The log utilitary to display messages
 * @returns A promise that resolves when the file is successfully downloaded and saved.
 * @throws If there is an error writing the file or fetching the dataset.
 */
const downloadResourceFile = async (filePath: string, { catalogConfig, resourceId, log }: GetResourceContext<DataFairConfig>): Promise<void> => {
  const url = `${catalogConfig.url}/data-fair/api/v1/datasets/${resourceId}/full`
  log.info('Import des données de la ressource', url)

  const fileStream = fs.createWriteStream(filePath)

  const response = await axios.get(url, { responseType: 'stream' })

  if (response.status !== 200) {
    throw new Error(`Error while fetching data: HTTP ${response.statusText}`)
  }

  return new Promise<void>((resolve, reject) => {
    response.data.pipe(fileStream)
    fileStream.on('finish', () => {
      resolve()
    })
    response.data.on('error', (err: any) => {
      fs.unlink(filePath, () => { }) // Delete the file in case of error
      reject(err)
    })
    fileStream.on('error', (err) => {
      fs.unlink(filePath, () => { }) // Delete the file in case of error
      reject(err)
    })
  })
}

/**
 * Downloads the rows of a dataset matching the given filters and saves them as a CSV file in a given file path.
 * @param filePath - The path to the temporary file where the CSV will be saved.
 * @param catalogConfig - The DataFair configuration object.
 * @param resourceId - The Id of the dataset to download.
 * @param importConfig - The import configuration, including filters to apply.
 * @param log - The log utilitary to display messages
 * @returns A promise that resolves when the file is successfully downloaded and saved.
 * @throws If there is an error writing the file or fetching the dataset.
 */
const downloadResourceLines = async (destFile: string, { catalogConfig, resourceId, importConfig, log }: GetResourceContext<DataFairConfig> & { importConfig: ImportConfig }) => {
  let url: string | null = `${catalogConfig.url}/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=3000`

  if (importConfig.fields) {
    url += '&select=' + importConfig.fields.map(field => field.key).join(',')
  }

  if (importConfig.filters) {
    importConfig.filters.forEach((filter) => {
      switch (filter.type) {
        case 'in':
        case 'nin':
          url += `&${filter.field.key}_${filter.type}="${filter.values?.join('","')}"`
          break
        case 'starts':
        case 'gte':
        case 'lte':
          url += `&${filter.field.key}_${filter.type}=${filter.value}`
          break
        default:
          break
      }
    })
  }

  log.info('Import des données de la ressource', url)
  const writer = fs.createWriteStream(destFile)
  let isFirstChunk = true

  while (url) {
    console.log(url)
    const response = await axios.get(url, { responseType: 'stream' })
    if (response.status !== 200) {
      throw new Error(`Error while fetching data: HTTP ${response.statusText}`)
    }

    await new Promise<void>((resolve, reject) => {
      let stream = response.data
      if (!isFirstChunk) {
        let skippedHeader = false
        stream = response.data.pipe(new Transform({
          transform (chunk, _encoding, callback) {
            if (!skippedHeader) {
              const headerEndIndex = chunk.indexOf('\n')
              if (headerEndIndex !== -1) {
                chunk = chunk.slice(headerEndIndex + 1)
                skippedHeader = true
              }
            }
            this.push(chunk)
            callback()
          }
        }))
      }
      stream.pipe(writer, { end: false })
      stream.on('end', () => {
        const linkHeader = response.headers.link
        url = extractNextPageUrl(linkHeader)
        isFirstChunk = false
        resolve()
      })
      stream.on('error', (error: any) => {
        writer.close()
        console.error(`Error while fetching lines at ${url}`, error)
        reject(error)
      })
    })
  }
  writer.end()
}

/**
 * Extract the next url (to fetch the next page) from the headers.
 * @param linkHeader the header where the `next` url should be
 * @returns the url if exists, null otherwise
 */
const extractNextPageUrl = (linkHeader: string | undefined): string | null => {
  if (!linkHeader) return null
  const links = linkHeader.split(',')
  for (const link of links) {
    const [urlPart, relPart] = link.split(';')
    const url = urlPart.trim().slice(1, -1) // Remove < and >
    const rel = relPart.trim()
    if (rel === 'rel=next') {
      return url
    }
  }
  return null
}
