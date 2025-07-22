import type { PrepareContext } from '@data-fair/types-catalogs'
import type { DataFairCapabilities } from './capabilities.ts'
import type { DataFairConfig } from '#types'
import axios from '@data-fair/lib-node/axios.js'

export default async ({ catalogConfig, capabilities, secrets }: PrepareContext<DataFairConfig, DataFairCapabilities>) => {
  // test the url
  try {
    await axios.get(catalogConfig.url + '/data-fair/api/v1/catalog/datasets?size=1&select=id')
  } catch (e) {
    console.error('Erreur URL pendant la configuration : ', e instanceof Error ? e.message : e)
    throw new Error('Configuration invalide, vérifiez l\'URL')
  }

  return {
    catalogConfig,
    capabilities,
    secrets
  }
}
