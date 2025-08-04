import type { DataFairConfig, DataFairDataset, ImportConfig } from '#types'
import type { CatalogPlugin, GetResourceContext, Resource } from '@data-fair/types-catalogs'
import axios from '@data-fair/lib-node/axios.js'
import * as fs from 'fs'
import { join } from 'path'
import { Transform } from 'stream'
import slugify from 'slugify'

/**
 * Retrieves a resource by first fetching its metadata and then downloading the actual resource.
 * The downloaded file path is added to the dataset metadata before returning.
 *
 * @param context - The context containing configuration and parameters required to fetch and download the resource.
 * @returns A promise that resolves to the dataset metadata with the downloaded file path included.
 */
export const getResource = async (context: GetResourceContext<DataFairConfig>): ReturnType<CatalogPlugin['getResource']> => {
  context.log.step('Import de la ressource')

  const { resource, file } = await getMetaData(context)
  resource.filePath = await downloadResource(context, file, resource)

  return resource
}

/**
 * Returns the DataFair Resource with all its metadatas
 * @param catalogConfig the DataFair configuration [ex: { url: 'https://example.com' }]
 * @param resourceId the dataset Id to fetch fields from
 * @returns the Resource corresponding to the id by this configuration
 */
const getMetaData = async ({ catalogConfig, resourceId, log, secrets }: GetResourceContext<DataFairConfig>): Promise<{ resource: Resource, file: boolean }> => {
  let dataset: DataFairDataset
  try {
    const url = `${catalogConfig.url}/data-fair/api/v1/datasets/${resourceId}`
    const config = secrets.apiKey ? { headers: { 'x-apiKey': secrets.apiKey } } : undefined
    const res = (await axios.get(url, config))
    if (res.status !== 200) {
      throw new Error(`HTTP error : ${res.status}, ${res.data}`)
    }
    dataset = res.data
    log.info('Import des métadonnées de la ressource', { url })
  } catch (e) {
    console.error('Error while fetching metadatas', e)
    throw new Error(`Erreur lors de la récuperation de la resource DataFair. ${e instanceof Error ? e.message : e}`)
  }

  dataset.schema = (dataset.schema ?? []).map((field) => {
    if (field['x-extension']) {
      return {
        ...field,
        key: slugify.default(field.key.replace(/^_/, ''), { lower: true, strict: true, replacement: '_' }), // Ensure no leading underscore
        'x-extension': undefined,   // Remove x-extension property if it exists
      }
    }
    return field
  })

  const resource: Resource = {
    id: resourceId,
    title: dataset.title,
    description: dataset.description,
    format: 'csv',
    origin: `${catalogConfig.url}/datasets/${resourceId}`,
    frequency: dataset.frequency,
    image: dataset.image,
    keywords: dataset.keywords,
    size: dataset.file?.size ?? dataset.storage?.size ?? dataset.originalFile?.size,
    schema: dataset.schema,
    filePath: '',
  }

  if (dataset.license) {
    resource.license = {
      title: dataset.license.title ?? '',
      href: dataset.license.href ?? '',
    }
  }

  return { resource, file: !!dataset.file }
}

/**
 * Download a specified from a Data Fair service.
 * If the resource has a distant file and no import configuration will download the distant file, otherwise the data will be fetch by set of rows.
 * @param context - the download context, contains the download configuration, the resource Id
 * @param res - the metadatas about the resource.
 * @returns A promise resolving to the file path of the downloaded CSV.
 */
const downloadResource = async (context: GetResourceContext<DataFairConfig>, file: boolean, res: Resource): Promise<string> => {
  const filePath = join(context.tmpDir, `${context.resourceId}.csv`)
  try {
    if (file && !context.importConfig.fields?.length && !context.importConfig.filters?.length) {
      await context.log.task('downloading', 'Téléchargement en cours... [file]', res.size || NaN)
      await downloadResourceFile(filePath, context)
    } else {
      await context.log.task('downloading', 'Téléchargement en cours... [lines]', NaN)
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
const downloadResourceFile = async (filePath: string, { catalogConfig, resourceId, log, secrets }: GetResourceContext<DataFairConfig>): Promise<void> => {
  const url = `${catalogConfig.url}/data-fair/api/v1/datasets/${resourceId}/full`
  const headers = secrets.apiKey ? { 'x-apiKey': secrets.apiKey } : undefined

  const response = await axios.get(url, { responseType: 'stream', headers })

  if (response.status !== 200) {
    throw new Error(`Error while fetching data: HTTP ${response.statusText}`)
  }

  let downloaded = 0
  let logPromise: Promise<void> | null = null

  return new Promise<void>((resolve, reject) => {
    const fileStream = fs.createWriteStream(filePath, { encoding: 'binary' }) // Ensure binary encoding

    response.data.on('data', (chunk: Buffer) => {
      downloaded += chunk.length
      if (!logPromise) {
        logPromise = log.progress('downloading', downloaded)
          .catch(err => console.warn('Progress logging failed:', err))
          .finally(() => { logPromise = null })
      }
    })

    response.data.pipe(fileStream)

    fileStream.on('finish', () => {
      fileStream.close()
      resolve()
    })

    response.data.on('error', (err: any) => {
      fileStream.destroy()
      fs.unlink(filePath, () => { })
      reject(err)
    })

    fileStream.on('error', (err) => {
      response.data.destroy()
      fs.unlink(filePath, () => { })
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
const downloadResourceLines = async (destFile: string, { catalogConfig, resourceId, importConfig, secrets, log }: GetResourceContext<DataFairConfig> & { importConfig: ImportConfig }) => {
  let url: string | null = `${catalogConfig.url}/data-fair/api/v1/datasets/${resourceId}/lines?format=csv&size=5000`

  if (importConfig.fields) {
    url += '&select=' + importConfig.fields.map(field => field.key).join(',')
  }

  const headers = secrets.apiKey ? { 'x-apiKey': secrets.apiKey } : undefined

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

  let downloaded = 0
  let pendingLogPromise: Promise<void> | null = null

  const writer = fs.createWriteStream(destFile)
  let isFirstChunk = true

  while (url) {
    console.log(`Fetching data from ${url}`)
    const response = await axios.get(url, { responseType: 'stream', headers })
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

      stream.on('data', (chunk: Buffer) => {
        downloaded += chunk.length

        // Only start a new log promise if there isn't one already running
        if (!pendingLogPromise) {
          pendingLogPromise = log.progress('downloading', downloaded)
            .catch(err => console.warn('Progress logging failed:', err))
            .finally(() => { pendingLogPromise = null })
        }
      })

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
