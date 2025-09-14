import { Job, QueueEvents } from 'bullmq'
import { ChainStepResponse, LogSources, StreamType } from '@latitude-data/constants'
import { documentsQueue } from '../../jobs/queues'
import { RunDocumentApiJobData } from '../../jobs/job-definitions/documents/runDocumentApiJob'
import { Result, TypedResult, OkType } from '../../lib/Result'
import { createPromiseWithResolver } from '../../lib/streamManager/utils/createPromiseResolver'
import { runDocumentAtCommit } from '../commits'
import { LatitudeError } from '@latitude-data/constants/errors'
import { ToolCall } from '@latitude-data/constants/legacyCompiler'

export type ApiRunResult = {
  error: Promise<Awaited<ReturnType<typeof runDocumentAtCommit>>['error']>
  lastResponse: OkType<typeof runDocumentAtCommit>['lastResponse']
  toolCalls: OkType<typeof runDocumentAtCommit>['toolCalls']
}

export type EventHandlers = {
  onProgress?: (event: { event: string; data: any }) => void
  onCompleted?: (data: { response: any; toolCalls: any[] }) => void
  onFailed?: (error: LatitudeError) => void
}

const queueEvents = new QueueEvents(documentsQueue.name)

export async function enqueueDocumentRunJob({
  workspaceId,
  documentUuid,
  commitUuid,
  projectId,
  parameters,
  customIdentifier,
  tools = [],
  userMessage,
  source = LogSources.API,
  eventHandlers,
}: {
  workspaceId: number
  documentUuid: string
  commitUuid: string
  projectId: number
  parameters?: Record<string, unknown>
  customIdentifier?: string
  tools?: string[]
  userMessage?: string
  source?: LogSources
  eventHandlers?: EventHandlers
}): Promise<
  TypedResult<
    {
      job: Job<RunDocumentApiJobData>
      queue: typeof documentsQueue
      result: ApiRunResult
    },
    Error
  >
> {
  const jobId = `workspace:${workspaceId}:commit:${commitUuid.slice(0, 7)}:document:${documentUuid}:${Date.now()}`

  try {
    const [promisedError, resolveError] =
      createPromiseWithResolver<
        Awaited<ReturnType<typeof runDocumentAtCommit>>['error']
      >()
    const [promisedResponse, resolveResponse] =
      createPromiseWithResolver<
        Awaited<OkType<typeof runDocumentAtCommit>['lastResponse']>
      >()
    const [promisedToolCalls, resolveToolCalls] =
      createPromiseWithResolver<
        Awaited<OkType<typeof runDocumentAtCommit>['toolCalls']>
      >()

    const job = await documentsQueue.add('runDocumentApiJob', {
      workspaceId,
      documentUuid,
      commitUuid,
      projectId,
      parameters,
      customIdentifier,
      tools,
      userMessage,
      source,
      jobId,
    } as RunDocumentApiJobData)
    if (!job.id) {
      return Result.error(new Error('Failed to enqueue document run job'))
    }

    // Send initial job queued event
    if (eventHandlers?.onProgress) {
      eventHandlers.onProgress({
        event: 'job_queued',
        data: { jobId: job.id! },
      })
    }

    // Listen for job progress updates on the queue
    const onProgress = ({
      jobId,
      data: progress,
    }: {
      jobId: string
      data: any
    }) => {
      if (jobId !== job.id!) return
      if (progress && typeof progress === 'object') {
        if (progress.type === 'stream_event') {
          eventHandlers?.onProgress?.({
            event: progress.event,
            data: progress.data,
          })
        } else if (progress.type === 'error') {
          eventHandlers?.onProgress?.({
            event: progress.event,
            data: progress.data,
          })
        }
      }
    }

    const onCompleted = ({
      jobId,
      returnvalue,
    }: {
      jobId: string
      returnvalue: string
    }) => {
      if (job.id !== jobId) return

      const { response, toolCalls } = JSON.parse(returnvalue) as { response: ChainStepResponse<StreamType>, toolCalls: ToolCall[] }

      resolveToolCalls(toolCalls)
      resolveResponse(response)
      resolveError(undefined)

      eventHandlers?.onCompleted?.({ response, toolCalls })

      cleanup()
    }

    const onFailed = ({
      failedReason,
      jobId,
    }: {
      failedReason: string
      jobId: string
    }) => {
      if (job.id !== jobId) return

      resolveToolCalls([])
      resolveResponse(undefined)
      const error = new LatitudeError(failedReason)
      resolveError(error)

      eventHandlers?.onFailed?.(error)
      cleanup()
    }

    const cleanup = () => {
      queueEvents
        .off('progress', onProgress)
        .off('completed', onCompleted)
        .off('failed', onFailed)
    }

    queueEvents.on('progress', onProgress)
    queueEvents.on('completed', onCompleted)
    queueEvents.on('failed', onFailed)

    return Result.ok({
      job,
      queue: documentsQueue,
      result: {
        toolCalls: promisedToolCalls,
        lastResponse: promisedResponse,
        error: promisedError,
      },
    })
  } catch (error) {
    return Result.error(error as Error)
  }
}
