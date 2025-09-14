import { Queues } from '@latitude-data/core/queues/types'
import * as jobs from '@latitude-data/core/jobs/definitions'
import { createWorker } from '../utils/createWorker'
import { WORKER_CONNECTION_CONFIG } from '../utils/connectionConfig'

const jobMappings = {
  runDocumentApiJob: jobs.runDocumentApiJob,
  runDocumentForExperimentJob: jobs.runDocumentForExperimentJob,
  runDocumentJob: jobs.runDocumentJob,
  runDocumentTriggerEventJob: jobs.runDocumentTriggerEventJob,
  runLatteJob: jobs.runLatteJob,
}

export function startDocumentsWorker() {
  return createWorker(Queues.documentsQueue, jobMappings, {
    concurrency: 25,
    connection: WORKER_CONNECTION_CONFIG,
  })
}
