export default {
  $id: 'https://github.com/data-fair/catalog-data-fair/catalog-schemas',
  'x-exports': [
    'types'
  ],
  title: 'DataFair Catalog',
  type: 'object',
  additionalProperties: false,
  required: [
    'count',
    'results'
  ],
  properties: {
    count: {
      type: 'number',
      description: 'Total number of datasets'
    },
    results: {
      type: 'array',
      items: {
        $ref: '#/$defs/DataFairDataset'
      }
    }
  },
  $defs: {
    DataFairDataset: {
      type: 'object',
      required: [
        'id',
        'title'
      ],
      properties: {
        id: {
          type: 'string',
          description: 'Globally unique identifier of the dataset'
        },
        href: {
          type: 'string',
          description: 'Readonly field. The URL where this resource can be fetched'
        },
        page: {
          type: 'string',
          description: 'Readonly field. The URL where this resource can be viewed in the UI'
        },

        title: {
          type: 'string',
          description: 'Short title of the dataset'
        },
        description: {
          type: 'string',
          description: 'Detailed description of the dataset'
        },

        keywords: {
          type: 'array',
          description: 'keywords',
          items: {
            type: 'string'
          }
        },
        frequency: {
          type: 'string',
          description: 'update frequency',
          enum: [
            '',
            'triennial',
            'biennial',
            'annual',
            'semiannual',
            'threeTimesAYear',
            'quarterly',
            'bimonthly',
            'monthly',
            'semimonthly',
            'biweekly',
            'threeTimesAMonth',
            'weekly',
            'semiweekly',
            'threeTimesAWeek',
            'daily',
            'continuous',
            'irregular'
          ]
        },
        image: {
          type: 'string',
          description: 'URL of an image illustrating the dataset'
        },
        license: {
          type: 'object',
          additionalProperties: false,
          required: [
            'title',
            'href'
          ],
          properties: {
            title: {
              type: 'string',
              description: 'Short title for the license'
            },
            href: {
              type: 'string',
              description: 'The URL where the license can be read'
            }
          }
        },

        modified: {
          type: 'string',
          format: 'date'
        },
        dataUpdatedAt: {
          type: 'string',
          format: 'date'
        },
        updatedAt: {
          type: 'string',
          format: 'date'
        },

        analysis: {
          type: 'object',
          properties: {
            escapeKeyAlgorithm: {
              type: 'string',
              enum: [
                'compat-ods'
              ]
            }
          }
        },
        projection: {
          type: 'object',
          properties: {
            code: { type: 'string' },
            title: { type: 'string' }
          }
        },

        file: {
          type: 'object',
          required: [
            'size'
          ],
          properties: {
            size: {
              type: 'number',
              description: 'Size of the file on disk'
            }
          }
        },
        originalFile: {
          type: 'object',
          required: [
            'size'
          ],
          properties: {
            size: {
              type: 'number',
              description: 'Size of the file on disk'
            }
          }
        },
        schema: {
          type: 'array',
          description: 'JSON schema properties of the fields',
          items: {
            type: 'object',
            required: [
              'key'
            ],
            properties: {
              key: {
                type: 'string',
                readOnly: true,
                'x-display': 'hidden'
              },
              type: {
                type: 'string'
              },
              format: {
                type: [
                  'string',
                  'null'
                ]
              },
              'x-originalName': {
                type: [
                  'string',
                  'null'
                ]
              },
              title: {
                type: 'string'
              },
              description: {
                type: 'string'
              },
              icon: {
                type: 'string'
              },
              'x-group': {
                type: 'string'
              },
              'x-refersTo': {
                deprecated: true,
                type: [
                  'string',
                  'null'
                ]
              },
              'x-concept': {
                type: 'object',
                properties: {
                  id: {
                    type: 'string'
                  },
                  title: {
                    type: 'string'
                  },
                  primary: {
                    type: 'boolean'
                  }
                }
              },
              'x-calculated': {
                type: 'boolean'
              },
              'x-capabilities': {
                type: 'object',
                properties: {
                  index: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Filterable on exact value',
                    description: 'Disable this capability if the data contains, for example, long texts for which filters on exact values make little sense.'
                  },
                  values: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Sortable and groupable',
                    description: 'Disable this capability if the data contains, for example, long texts for which sorting or grouping by value makes little sense.'
                  },
                  textStandard: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Text analyzed for full-text search',
                    description: 'Disable this capability for a code, a URL, etc. Any content for which word search makes little sense.'
                  },
                  text: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Text analyzed specifically for the French language',
                    description: 'Disable this capability for any content that is not in French or for which word search makes little sense.'
                  },
                  textAgg: {
                    type: 'boolean',
                    default: false,
                    'x-display': 'switch',
                    title: 'Word statistics',
                    description: 'Enable this capability if you intend to obtain statistics on word occurrences (for example to build a word cloud).'
                  },
                  wildcard: {
                    type: 'boolean',
                    default: false,
                    'x-display': 'switch',
                    title: 'Text filterable on character group',
                    description: 'Enable this capability if you intend to filter this content specifically on a sequence of characters (for example if a filter on whole words or exact value is not suitable).'
                  },
                  insensitive: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Improved sorting with case and accents',
                    description: 'Disable this capability if the content will not be used for sorting or if it does not contain variations with accents and uppercase letters.'
                  },
                  geoShape: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Complex geometric shapes',
                    description: 'Disable this capability if the data only contains basic point geometries or if querying geometries solely from their centroids is sufficient for your needs.'
                  },
                  vtPrepare: {
                    type: 'boolean',
                    default: false,
                    'x-display': 'switch',
                    title: 'Prepared vector tiles',
                    description: 'Enable this capability to precompute elements useful for building vector tiles for the map rendering of the dataset. Enable this option if the dataset contains dense geographic data to be displayed in large quantities. The trade-off is an increase in indexing time and in the volume of indexed data.'
                  },
                  indexAttachment: {
                    type: 'boolean',
                    default: true,
                    'x-display': 'switch',
                    title: 'Attachment content analyzed for full-text search',
                    description: 'Disable this option if you want attachments to be simply downloadable and the extraction of their textual content for word search is not relevant.'
                  }
                }
              },
              'x-labels': {
                type: 'object',
                patternProperties: {
                  '.*': {
                    type: 'string'
                  }
                }
              },
              'x-labelsRestricted': {
                type: 'boolean'
              },
              readOnly: {
                type: 'boolean'
              },
              'x-required': {
                type: 'boolean'
              },
              minLength: {
                type: 'integer'
              },
              maxLength: {
                type: 'integer'
              },
              minimum: {
                type: 'number'
              },
              maximum: {
                type: 'number'
              },
              pattern: {
                type: 'string',
                format: 'regex'
              },
              patternErrorMessage: {
                type: 'string'
              },
              'x-master': {
                type: 'object',
                properties: {
                  id: {
                    type: 'string'
                  },
                  title: {
                    type: 'string'
                  },
                  remoteService: {
                    type: 'string',
                    description: 'The identifier of the remote service used for enrichment'
                  },
                  action: {
                    type: 'string',
                    description: 'The identifier of the remote service action to use for enrichment'
                  }
                }
              },
              'x-display': {
                type: 'string'
              },
              enum: {
                type: 'array',
                readOnly: true,
                description: 'This differs from JSON schema. It is not a restriction, just and observation of the values that are present in the dataset.'
              },
              'x-cardinality': {
                type: 'integer',
                description: 'The number of distinct values for this field',
                readOnly: true
              },
              'x-transform': {
                type: 'object',
                description: 'Transformation to apply to the field',
                properties: {
                  expr: {
                    type: 'string'
                  },
                  examples: {
                    type: 'array',
                    items: {
                      type: 'string'
                    }
                  },
                  type: {
                    type: 'string'
                  },
                  format: {
                    type: 'string'
                  }
                }
              }
            }
          }
        },
        storage: {
          type: 'object',
          description: 'All storage space info of this dataset',
          properties: {
            size: {
              type: 'integer'
            },
            dataFiles: {
              type: 'array',
              description: 'The array of data files.',
              items: {
                type: 'object',
                properties: {
                  key: {
                    type: 'string'
                  },
                  size: {
                    type: 'number'
                  },
                  name: {
                    type: 'string'
                  },
                  mimetype: {
                    type: 'string'
                  },
                  updatedAt: {
                    type: 'string',
                    format: 'date-time'
                  },
                  title: {
                    type: 'string'
                  },
                  url: {
                    type: 'string'
                  }
                }
              }
            }
          }
        },
        count: {
          type: 'number',
          description: 'The number of rows'
        }
      }
    }
  }
}
