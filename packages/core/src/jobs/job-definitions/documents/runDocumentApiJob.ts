import { Job } from 'bullmq'

import { LogSources } from '@latitude-data/constants'
import { BACKGROUND } from '../../../telemetry'
import {
  runDocumentAtCommit,
} from '../../../services/commits/runDocumentAtCommit'

import { getDataForInitialRequest } from './runDocumentAtCommitWithAutoToolResponses/getDataForInitialRequest'
import {
  awaitClientToolResult,
  type ToolHandler,
} from '../../../lib/streamManager/clientTools/handlers'
import { publisher } from '../../../events/publisher'

export type RunDocumentApiJobData = {
  workspaceId: number
  documentUuid: string
  commitUuid: string
  projectId: number
  parameters?: Record<string, unknown>
  customIdentifier?: string
  tools?: string[]
  userMessage?: string
  source?: LogSources
  jobId: string // For tracking and callbacks
}

export type DocumentRunEvent = {
  event: string
  data: unknown
  jobId: string
}

function buildClientToolHandlersMap(
  tools: string[],
): Record<string, ToolHandler> {
  return tools.reduce((acc: Record<string, ToolHandler>, toolName: string) => {
    acc[toolName] = awaitClientToolResult
    return acc
  }, {})
}

export const runDocumentApiJob = async (job: Job<RunDocumentApiJobData>) => {
  const {
    workspaceId,
    documentUuid,
    commitUuid,
    projectId,
    parameters = {},
    customIdentifier,
    tools = [],
    userMessage,
    source = LogSources.API,
  } = job.data

  // Get the document and commit data
  const { workspace, document, commit } = await getDataForInitialRequest({
    workspaceId,
    projectId,
    documentUuid,
    commitUuid,
  }).then((r) => r.unwrap())

  // Build tool handlers map
  const toolHandlers = buildClientToolHandlersMap(tools)

  // Handle abort signal
  const abortController = new AbortController()
  publisher.subscribe('cancelJob', ({ jobId }: { jobId: string }) => {
    if (jobId === job.id) {
      abortController.abort()
    }
  })

  // Run the document
  const result = await runDocumentAtCommit({
    workspace,
    document,
    commit,
    source,
    parameters,
    userMessage,
    customIdentifier,
    context: BACKGROUND({ workspaceId }),
    tools: toolHandlers,
    abortSignal: abortController.signal,
  }).then((r) => r.unwrap())

  // Broadcast stream events
  const reader = result.stream.getReader()
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      // Update job progress with the actual event data
      job.updateProgress({
        type: 'stream_event',
        event: value.event,
        data: value.data,
      })
    }
  } finally {
    reader.releaseLock()
  }

  // BullMQ serializes return values with the most basic of strategies so
  // unless we stringify here we would get an "[Object object]" on the other
  // side
  return JSON.stringify({
    success: true,
    response: await result.lastResponse,
    toolCalls: await result.toolCalls,
  })
}
