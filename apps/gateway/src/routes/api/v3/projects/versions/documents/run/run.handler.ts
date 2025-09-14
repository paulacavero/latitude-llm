import {
  getData,
  publishDocumentRunRequestedEvent,
} from '$/common/documents/getData'
import { captureException } from '$/common/sentry'
import { AppRouteHandler } from '$/openApi/types'
import { runPresenter, runPresenterLegacy } from '$/presenters/runPresenter'
import { LogSources } from '@latitude-data/core/browser'
import { getUnknownError } from '@latitude-data/core/lib/getUnknownError'
import { isAbortError } from '@latitude-data/core/lib/isAbortError'
import { enqueueDocumentRunJob } from '@latitude-data/core/services/documents/enqueueDocumentRunJob'
import { streamSSE } from 'hono/streaming'
import { RunRoute } from './run.route'
import {
  awaitClientToolResult,
  ToolHandler,
} from '@latitude-data/core/lib/streamManager/clientTools/handlers'

import { compareVersion } from '$/utils/versionComparison'
import { publisher } from '@latitude-data/core/events/publisher'

type RunHandlerContext = {
  c: Parameters<AppRouteHandler<RunRoute>>[0]
  workspace: any
  document: any
  commit: any
  project: any
  projectId: string
  versionUuid: string
  path: string
  parameters?: Record<string, unknown>
  customIdentifier?: string
  tools?: string[]
  userMessage?: string
  __internal?: any
  sdkVersion?: string
  isLegacy: boolean
}

// https://github.com/honojs/middleware/issues/735
// https://github.com/orgs/honojs/discussions/1803
// @ts-expect-error: streamSSE has type issues with zod-openapi
export const runHandler: AppRouteHandler<RunRoute> = async (c) => {
  const context = await buildRunHandlerContext(c)
  const { stream: useSSE } = c.req.valid('json')
  if (useSSE) {
    return handleStreamingMode(context)
  }

  return c.json(await handleNonStreamingMode(context))
}

export function buildClientToolHandlersMap(tools: string[]) {
  return tools.reduce((acc: Record<string, ToolHandler>, toolName: string) => {
    acc[toolName] = awaitClientToolResult
    return acc
  }, {})
}

async function buildRunHandlerContext(
  c: Parameters<AppRouteHandler<RunRoute>>[0],
): Promise<RunHandlerContext> {
  const { projectId, versionUuid } = c.req.valid('param')
  const { path, parameters, customIdentifier, tools, userMessage, __internal } =
    c.req.valid('json')
  const workspace = c.get('workspace')
  const { document, commit, project } = await getData({
    workspace,
    projectId: Number(projectId!),
    commitUuid: versionUuid!,
    documentPath: path!,
  }).then((r) => r.unwrap())

  if (__internal?.source === LogSources.API) {
    await publishDocumentRunRequestedEvent({
      workspace,
      project,
      commit,
      document,
      parameters,
    })
  }

  const sdkVersion = c.req.header('X-Latitude-SDK-Version')
  const isLegacy = !compareVersion(sdkVersion, '5.0.0')

  return {
    c,
    workspace,
    document,
    commit,
    project,
    projectId: projectId!,
    versionUuid: versionUuid!,
    path: path!,
    parameters,
    customIdentifier,
    tools,
    userMessage,
    __internal,
    sdkVersion,
    isLegacy,
  }
}

async function handleStreamingMode(context: RunHandlerContext) {
  const {
    c,
    workspace,
    projectId,
    versionUuid,
    document,
    parameters,
    customIdentifier,
    tools,
    userMessage,
    __internal,
  } = context

  return streamSSE(
    c,
    async (stream) => {
      let id = 0

      // For background runs, enqueue a job instead of running directly
      const { job, result } = await enqueueDocumentRunJob({
        workspaceId: workspace.id,
        documentUuid: document.documentUuid,
        commitUuid: versionUuid,
        projectId: Number(projectId),
        parameters,
        customIdentifier,
        tools: tools ?? [],
        userMessage,
        source: __internal?.source ?? LogSources.API,
        eventHandlers: {
          onProgress: (event) => {
            stream.writeSSE({
              id: String(id++),
              event: event.event,
              data:
                typeof event.data === 'string'
                  ? event.data
                  : JSON.stringify(event.data),
            })
          },
          onFailed: async (error) => {
            await stream.writeSSE({
              id: String(id++),
              event: 'error',
              data: JSON.stringify({ error: error.message }),
            })
            stream.close()
          },
        },
      }).then((r) => r.unwrap())

      // this is just here to make sure that below gets triggered
      stream.onAbort(() => {
        publisher.publish('cancelJob', { jobId: job.id })
      })

      // Wait for the job to complete or fail
      try {
        await result.error
      } catch (error) {
        // Error already handled by onFailed handler
      }
    },
    (error: Error) => {
      // Don't log abort errors as they are expected when clients disconnect
      if (isAbortError(error)) return Promise.resolve()

      const unknownError = getUnknownError(error)
      if (unknownError) captureException(error)

      return Promise.resolve()
    },
  )
}

async function handleNonStreamingMode(context: RunHandlerContext) {
  const {
    workspace,
    projectId,
    versionUuid,
    path,
    parameters,
    customIdentifier,
    userMessage,
    __internal,
    isLegacy,
  } = context

  // For non-SSE requests, enqueue job without event handlers
  const { result } = await enqueueDocumentRunJob({
    workspaceId: workspace.id,
    documentUuid: path,
    commitUuid: versionUuid,
    projectId: Number(projectId),
    parameters,
    customIdentifier,
    tools: [],
    userMessage,
    source: __internal?.source ?? LogSources.API,
  }).then((r) => r.unwrap())

  const error = await result.error
  if (error) throw error

  let body
  if (isLegacy) {
    body = runPresenterLegacy({
      response: (await result.lastResponse)!,
      toolCalls: await result.toolCalls
    }).unwrap()
  } else {
    body = runPresenter({
      response: (await result.lastResponse)!,
    }).unwrap()
  }

  return body
}
