import type { PrepareContext } from '@data-fair/types-catalogs'
import type { DataFairCapabilities } from './capabilities.ts'
import type { DataFairConfig } from '#types'
import axios from '@data-fair/lib-node/axios.js'

export default async ({ catalogConfig, capabilities, secrets }: PrepareContext<DataFairConfig, DataFairCapabilities>) => {
  // set the apiKey in the secrets field if it exists
  const apiKey = catalogConfig.apiKey
  if (apiKey && apiKey !== '*************************') {
    secrets.apiKey = apiKey
    catalogConfig.apiKey = '*************************'
  } else if (secrets?.apiKey && (!apiKey || apiKey === '')) {
    delete secrets.apiKey
  } else {
    // The secret is already set, do nothing
  }

  // test the url
  try {
    if (!catalogConfig.url) {
      throw new Error('URL du catalogue non définie')
    }
    const config = secrets.apiKey ? { headers: { 'x-apiKey': secrets.apiKey } } : undefined
    await axios.get(catalogConfig.url + '/data-fair/api/v1/catalog/datasets?size=1&select=id', config)
  } catch (e) {
    console.error('Erreur URL pendant la configuration : ', e instanceof Error ? e.message : e)
    throw new Error(`Configuration invalide, veuillez vérifier l’URL du catalogue et la clé API si nécessaire (${e instanceof Error ? e.message : e})`)
  }

  return {
    catalogConfig,
    capabilities,
    secrets
  }
}
