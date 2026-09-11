import {
  ApprovalRequestId,
  type KimiSettings,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { mapAcpToAdapterError, acpPermissionOutcome } from "../acp/AcpAdapterSupport.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import { makeAcpNativeLoggerFactory } from "../acp/AcpNativeLogging.ts";
import { makeKimiAcpRuntime, resolveKimiAcpBaseModelId } from "../acp/KimiAcpSupport.ts";
import { type KimiAdapterShape } from "../Services/KimiAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const encodeUnknownJsonStringExit = Schema.encodeUnknownExit(Schema.fromJsonString(Schema.Unknown));

const PROVIDER = ProviderDriverKind.make("kimi");
const KIMI_RESUME_VERSION = 1 as const;
const NANOS_PER_MILLI = 1_000_000n;
const DEFAULT_KIMI_TURN_INACTIVITY_TIMEOUT_MS = 10 * 60 * 1_000;
const DEFAULT_KIMI_ACTIVE_TOOL_INACTIVITY_TIMEOUT_MS = 30 * 60 * 1_000;

function encodeJsonStringForDiagnostics(input: unknown): string | undefined {
  const result = encodeUnknownJsonStringExit(input);
  return Exit.isSuccess(result) ? result.value : undefined;
}

export interface KimiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
  readonly turnInactivityTimeoutMs?: number;
  readonly activeToolInactivityTimeoutMs?: number;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

type PendingUserInputResolution =
  | { readonly _tag: "answered"; readonly answers: ProviderUserInputAnswers }
  | { readonly _tag: "cancelled" };

interface PendingUserInput {
  readonly resolution: Deferred.Deferred<PendingUserInputResolution>;
}

interface KimiTurnLivenessSignal {
  readonly turnId: TurnId;
}

interface KimiSessionContext {
  readonly threadId: ThreadId;
  readonly acpSessionId: string;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  lastPlanFingerprint: string | undefined;
  activeTurnId: TurnId | undefined;
  interruptedTurnIds: Set<TurnId>;
  promptsInFlight: number;
  promptEpoch: number;
  discardBeforeEpoch: number;
  readonly promptLifecycle: Semaphore.Semaphore;
  readonly livenessSignals: Queue.Queue<KimiTurnLivenessSignal>;
  livenessTurnId: TurnId | undefined;
  lastTurnActivityAtNanos: bigint | undefined;
  readonly activeToolCallIds: Set<string>;
  livenessUpdatesInFlight: number;
  promptResponsesReady: number;
  currentModelId: string | undefined;
  stopped: boolean;
}

function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingApprovals.values()),
    (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
    { discard: true },
  );
}

function settlePendingUserInputsAsCancelled(
  pendingUserInputs: ReadonlyMap<ApprovalRequestId, PendingUserInput>,
): Effect.Effect<void> {
  return Effect.forEach(
    Array.from(pendingUserInputs.values()),
    (pending) => Deferred.succeed(pending.resolution, { _tag: "cancelled" }).pipe(Effect.ignore),
    { discard: true },
  );
}

function appendPromptResultToTurn(
  ctx: KimiSessionContext,
  turnId: TurnId,
  promptParts: ReadonlyArray<EffectAcpSchema.ContentBlock>,
  result: EffectAcpSchema.PromptResponse,
): void {
  const existingTurnRecord = ctx.turns.find((turn) => turn.id === turnId);
  ctx.turns = existingTurnRecord
    ? ctx.turns.map((turn) =>
        turn.id === turnId
          ? { ...turn, items: [...turn.items, { prompt: promptParts, result }] }
          : turn,
      )
    : [...ctx.turns, { id: turnId, items: [{ prompt: promptParts, result }] }];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const resolveNotificationTurnId = (ctx: KimiSessionContext): TurnId | undefined => ctx.activeTurnId;

function parseKimiResume(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== KIMI_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { sessionId: raw.sessionId.trim() };
}

function selectPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const preferredKind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  const preferred = request.options.find((entry) => entry.kind === preferredKind);
  const preferredId = preferred?.optionId.trim();
  if (preferredId) return preferredId;
  if (decision === "acceptForSession") {
    const once = request.options.find((entry) => entry.kind === "allow_once");
    return once?.optionId.trim();
  }
  return undefined;
}

function selectAutoApprovedPermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    selectPermissionOptionId(request, "acceptForSession") ??
    selectPermissionOptionId(request, "accept")
  );
}

export function makeKimiAdapter(kimiSettings: KimiSettings, options?: KimiAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("kimi");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
    const makeAcpNativeLoggers = yield* makeAcpNativeLoggerFactory();

    const sessions = new Map<ThreadId, KimiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
    const requestedTurnInactivityTimeoutMs = options?.turnInactivityTimeoutMs;
    const turnInactivityTimeoutMs =
      typeof requestedTurnInactivityTimeoutMs === "number" &&
      Number.isFinite(requestedTurnInactivityTimeoutMs)
        ? Math.max(1, Math.floor(requestedTurnInactivityTimeoutMs))
        : DEFAULT_KIMI_TURN_INACTIVITY_TIMEOUT_MS;
    const turnInactivityTimeoutNanos = BigInt(turnInactivityTimeoutMs) * NANOS_PER_MILLI;
    const requestedActiveToolInactivityTimeoutMs = options?.activeToolInactivityTimeoutMs;
    const activeToolInactivityTimeoutMs =
      typeof requestedActiveToolInactivityTimeoutMs === "number" &&
      Number.isFinite(requestedActiveToolInactivityTimeoutMs)
        ? Math.max(1, Math.floor(requestedActiveToolInactivityTimeoutMs))
        : DEFAULT_KIMI_ACTIVE_TOOL_INACTIVITY_TIMEOUT_MS;
    const activeToolInactivityTimeoutNanos =
      BigInt(activeToolInactivityTimeoutMs) * NANOS_PER_MILLI;

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Kimi runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });
    const mapAcpCallbackFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new EffectAcpErrors.AcpTransportError({
              detail: "Failed to process Kimi ACP callback.",
              cause,
            }),
        ),
      );

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const signalTurnLiveness = (ctx: KimiSessionContext, turnId: TurnId) =>
      Queue.offer(ctx.livenessSignals, { turnId }).pipe(Effect.asVoid);

    const beginTurnLiveness = (ctx: KimiSessionContext, turnId: TurnId) =>
      Effect.sync(() => {
        ctx.livenessTurnId = turnId;
        ctx.lastTurnActivityAtNanos = undefined;
        ctx.activeToolCallIds.clear();
      });

    const clearTurnLiveness = (ctx: KimiSessionContext) => {
      const turnId = ctx.livenessTurnId;
      ctx.livenessTurnId = undefined;
      ctx.lastTurnActivityAtNanos = undefined;
      ctx.activeToolCallIds.clear();
      ctx.livenessUpdatesInFlight = 0;
      ctx.promptResponsesReady = 0;
      return turnId === undefined ? Effect.void : signalTurnLiveness(ctx, turnId);
    };

    const recordTurnActivity = Effect.fn("KimiAdapter.recordTurnActivity")(function* (
      ctx: KimiSessionContext,
      turnId: TurnId,
      event: Extract<
        AcpSessionRuntime.AcpSessionRuntimeEvent,
        {
          _tag:
            | "AssistantItemStarted"
            | "AssistantItemCompleted"
            | "PlanUpdated"
            | "ToolCallUpdated"
            | "ContentDelta";
        }
      >,
    ) {
      if (
        ctx.livenessTurnId !== turnId ||
        (event._tag === "ContentDelta" && event.text.length === 0)
      ) {
        return;
      }
      ctx.livenessUpdatesInFlight += 1;
      try {
        const activityAtNanos = yield* Clock.monotonicTimeNanos;
        if (ctx.livenessTurnId !== turnId || ctx.interruptedTurnIds.has(turnId)) {
          return;
        }
        if (event._tag === "ToolCallUpdated") {
          if (event.toolCall.status === "completed" || event.toolCall.status === "failed") {
            ctx.activeToolCallIds.delete(event.toolCall.toolCallId);
          } else {
            ctx.activeToolCallIds.add(event.toolCall.toolCallId);
          }
        }
        ctx.lastTurnActivityAtNanos = activityAtNanos;
      } finally {
        ctx.livenessUpdatesInFlight = Math.max(0, ctx.livenessUpdatesInFlight - 1);
        yield* signalTurnLiveness(ctx, turnId);
      }
    });

    const hasLivenessPause = (ctx: KimiSessionContext) =>
      ctx.pendingApprovals.size > 0 ||
      ctx.pendingUserInputs.size > 0 ||
      ctx.livenessUpdatesInFlight > 0;

    const livenessTimeoutFor = (ctx: KimiSessionContext) =>
      ctx.activeToolCallIds.size > 0
        ? {
            milliseconds: activeToolInactivityTimeoutMs,
            nanos: activeToolInactivityTimeoutNanos,
          }
        : { milliseconds: turnInactivityTimeoutMs, nanos: turnInactivityTimeoutNanos };

    const signalSessionTurnLiveness = (threadId: ThreadId, turnId: TurnId | undefined) => {
      const ctx = sessions.get(threadId);
      return ctx && turnId !== undefined ? signalTurnLiveness(ctx, turnId) : Effect.void;
    };

    const resumeSessionTurnLiveness = Effect.fn("KimiAdapter.resumeSessionTurnLiveness")(function* (
      threadId: ThreadId,
      turnId: TurnId | undefined,
    ) {
      const ctx = sessions.get(threadId);
      if (!ctx || turnId === undefined || ctx.livenessTurnId !== turnId) {
        return;
      }
      ctx.lastTurnActivityAtNanos = yield* Clock.monotonicTimeNanos;
      yield* signalTurnLiveness(ctx, turnId);
    });

    const refreshSessionTurnLiveness = Effect.fn("KimiAdapter.refreshSessionTurnLiveness")(
      function* (threadId: ThreadId, turnId: TurnId | undefined) {
        const ctx = sessions.get(threadId);
        if (
          !ctx ||
          turnId === undefined ||
          ctx.livenessTurnId !== turnId ||
          ctx.lastTurnActivityAtNanos === undefined
        ) {
          return;
        }
        ctx.lastTurnActivityAtNanos = yield* Clock.monotonicTimeNanos;
        yield* signalTurnLiveness(ctx, turnId);
      },
    );

    const markPromptResponseReady = Effect.fn("KimiAdapter.markPromptResponseReady")(function* (
      threadId: ThreadId,
      acpSessionId: string,
      turnId: TurnId,
    ) {
      const ctx = sessions.get(threadId);
      if (
        ctx &&
        ctx.acpSessionId === acpSessionId &&
        !ctx.stopped &&
        !ctx.interruptedTurnIds.has(turnId) &&
        ctx.livenessTurnId === turnId &&
        ctx.activeTurnId === turnId &&
        ctx.session.activeTurnId === turnId
      ) {
        ctx.promptResponsesReady += 1;
        yield* signalTurnLiveness(ctx, turnId);
      }
    });

    const consumePromptResponseReady = (ctx: KimiSessionContext) => {
      ctx.promptResponsesReady = Math.max(0, ctx.promptResponsesReady - 1);
    };

    const isLiveTurn = (ctx: KimiSessionContext, turnId: TurnId) =>
      ctx.promptsInFlight > 0 &&
      ctx.promptsInFlight > ctx.promptResponsesReady &&
      ctx.activeTurnId === turnId &&
      ctx.session.activeTurnId === turnId &&
      (ctx.session.status === "running" || ctx.session.status === "connecting");

    const settlePromptInFlight = (
      threadId: ThreadId,
      turnId: TurnId,
      expectedAcpSessionId: string,
      options?: {
        readonly errorMessage?: string;
        readonly completedStopReason?: EffectAcpSchema.StopReason | null;
        readonly emitTurnCompletion?: boolean;
        readonly settleAllPrompts?: boolean;
      },
    ) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(threadId);
        if (!liveCtx) return;
        if (
          liveCtx.acpSessionId !== expectedAcpSessionId ||
          (liveCtx.activeTurnId !== turnId && liveCtx.session.activeTurnId !== turnId)
        ) {
          if (
            liveCtx.acpSessionId !== expectedAcpSessionId ||
            liveCtx.interruptedTurnIds.has(turnId)
          ) {
            return;
          }
          if (options?.emitTurnCompletion !== false) {
            if (options?.errorMessage !== undefined) {
              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId,
                turnId,
                payload: { state: "failed", errorMessage: options.errorMessage },
              });
            } else if (options?.completedStopReason !== undefined) {
              yield* offerRuntimeEvent({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId,
                turnId,
                payload: {
                  state: options.completedStopReason === "cancelled" ? "cancelled" : "completed",
                  stopReason: options.completedStopReason ?? null,
                },
              });
            }
          }
          return;
        }
        if (options?.settleAllPrompts) {
          liveCtx.promptsInFlight = 0;
        } else {
          const remainingPrompts = Math.max(0, liveCtx.promptsInFlight - 1);
          if (remainingPrompts > 0) {
            liveCtx.promptsInFlight = remainingPrompts;
            return;
          }
          liveCtx.promptsInFlight = remainingPrompts;
        }
        yield* clearTurnLiveness(liveCtx);
        const updatedAt = yield* nowIso;
        const canEmitTurnCompletion =
          liveCtx.session.status === "running" || liveCtx.session.status === "connecting";
        const { activeTurnId: _activeTurnId, ...readySession } = liveCtx.session;
        liveCtx.activeTurnId = undefined;
        liveCtx.session = { ...readySession, status: "ready", updatedAt };
        if (options?.emitTurnCompletion === false) return;
        if (options?.errorMessage !== undefined && canEmitTurnCompletion) {
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId,
            payload: { state: "failed", errorMessage: options.errorMessage },
          });
        } else if (options?.completedStopReason !== undefined && canEmitTurnCompletion) {
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId,
            turnId,
            payload: {
              state: options.completedStopReason === "cancelled" ? "cancelled" : "completed",
              stopReason: options.completedStopReason ?? null,
            },
          });
        }
      });

    const settleStalledTurn = Effect.fn("KimiAdapter.settleStalledTurn")(function* (
      ctx: KimiSessionContext,
      turnId: TurnId,
    ) {
      return yield* withThreadLock(
        ctx.threadId,
        Effect.gen(function* () {
          const liveCtx = sessions.get(ctx.threadId);
          if (
            liveCtx !== ctx ||
            ctx.stopped ||
            !isLiveTurn(ctx, turnId) ||
            ctx.interruptedTurnIds.has(turnId) ||
            hasLivenessPause(ctx)
          ) {
            return;
          }
          const lastActivityAtNanos = ctx.lastTurnActivityAtNanos;
          if (lastActivityAtNanos === undefined) return;
          const nowNanos = yield* Clock.monotonicTimeNanos;
          if (
            ctx.interruptedTurnIds.has(turnId) ||
            !isLiveTurn(ctx, turnId) ||
            hasLivenessPause(ctx) ||
            nowNanos - lastActivityAtNanos < livenessTimeoutFor(ctx).nanos
          ) {
            return;
          }
          ctx.interruptedTurnIds.add(turnId);
          yield* Effect.ignore(
            ctx.acp.cancel.pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, ctx.threadId, "session/cancel", error),
              ),
            ),
          );
          yield* Effect.ignore(ctx.acp.drainEvents);
          yield* settlePromptInFlight(ctx.threadId, turnId, ctx.acpSessionId, {
            errorMessage: `Kimi ACP turn stalled without content or tool progress for ${livenessTimeoutFor(ctx).milliseconds}ms.`,
            settleAllPrompts: true,
          });
        }),
      );
    });

    const runTurnLivenessWatchdog = Effect.fn("KimiAdapter.runTurnLivenessWatchdog")(
      function* (ctx: KimiSessionContext) {
        while (true) {
          if (ctx.stopped) return;
          const turnId = ctx.livenessTurnId;
          if (
            turnId === undefined ||
            ctx.interruptedTurnIds.has(turnId) ||
            !isLiveTurn(ctx, turnId) ||
            hasLivenessPause(ctx)
          ) {
            yield* Queue.take(ctx.livenessSignals);
            continue;
          }
          const lastActivityAtNanos = ctx.lastTurnActivityAtNanos;
          if (lastActivityAtNanos === undefined) {
            yield* Queue.take(ctx.livenessSignals);
            continue;
          }
          const nowNanos = yield* Clock.monotonicTimeNanos;
          const remainingNanos = livenessTimeoutFor(ctx).nanos - (nowNanos - lastActivityAtNanos);
          if (remainingNanos <= 0n) {
            yield* settleStalledTurn(ctx, turnId);
            continue;
          }
          const wakeReason = yield* Effect.raceFirst(
            Effect.sleep(Duration.nanos(remainingNanos)).pipe(Effect.as("timeout" as const)),
            Queue.take(ctx.livenessSignals).pipe(Effect.as("activity" as const)),
          );
          if (wakeReason === "timeout") {
            yield* settleStalledTurn(ctx, turnId);
          }
        }
      },
      Effect.catch(() => Effect.void),
    );

    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to write native Kimi notification log.", {
            cause,
            threadId,
            method,
          }),
        ),
      );

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<KimiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: KimiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
        yield* settlePendingUserInputsAsCancelled(ctx.pendingUserInputs);
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const resolveSessionCallbackTurnId = (threadId: ThreadId): TurnId | undefined => {
      const ctx = sessions.get(threadId);
      return ctx?.activeTurnId;
    };

    const startSession: KimiAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = path.resolve(input.cwd.trim());
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
          const pendingUserInputs = new Map<ApprovalRequestId, PendingUserInput>();
          const sessionApprovedOperations = new Set<string>();
          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          const resumeSessionId = parseKimiResume(input.resumeCursor)?.sessionId;
          const acpNativeLoggers = makeAcpNativeLoggers({
            nativeEventLogger,
            provider: PROVIDER,
            threadId: input.threadId,
          });

          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          const acp = yield* makeKimiAcpRuntime({
            kimiSettings,
            ...(options?.environment || mcpSession?.agentDeviceEnvironment
              ? {
                  environment: McpProviderSession.withAgentDeviceEnvironment(
                    options?.environment ?? process.env,
                    mcpSession,
                  ),
                }
              : {}),
            childProcessSpawner,
            cwd,
            runtimeMode: input.runtimeMode,
            ...(resumeSessionId ? { resumeSessionId } : {}),
            clientInfo: { name: "t3-code", version: "0.0.0" },
            ...(mcpSession
              ? {
                  mcpServers: [
                    {
                      type: "http" as const,
                      name: "t3-code",
                      url: mcpSession.endpoint,
                      headers: [
                        {
                          name: "Authorization",
                          value: mcpSession.authorizationHeader,
                        },
                      ],
                    },
                  ],
                }
              : {}),
            ...acpNativeLoggers,
          }).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const started = yield* Effect.gen(function* () {
            yield* acp.handleRequestPermission((params) =>
              mapAcpCallbackFailure(
                Effect.gen(function* () {
                  yield* logNative(input.threadId, "session/request_permission", params);
                  const permissionRequest = parsePermissionRequest(params);
                  const command = permissionRequest.toolCall?.command;
                  const { kind, title, rawInput, locations } = params.toolCall;
                  let operationInput = rawInput;
                  if (isRecord(rawInput) && rawInput.variant === "Bash") {
                    const { description: _description, ...shellInput } = rawInput;
                    operationInput = shellInput;
                  }
                  const approvalKey =
                    command || (isRecord(rawInput) && Object.keys(rawInput).length > 0)
                      ? stableStringify({ kind, title, command, input: operationInput, locations })
                      : undefined;
                  const alreadyApproved =
                    approvalKey !== undefined && sessionApprovedOperations.has(approvalKey);
                  if (input.runtimeMode === "full-access" || alreadyApproved) {
                    const autoApprovedOptionId =
                      input.runtimeMode === "full-access"
                        ? selectAutoApprovedPermissionOption(params)
                        : selectPermissionOptionId(params, "accept");
                    if (autoApprovedOptionId !== undefined) {
                      return {
                        outcome: {
                          outcome: "selected" as const,
                          optionId: autoApprovedOptionId,
                        },
                      };
                    }
                  }
                  const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
                  const runtimeRequestId = RuntimeRequestId.make(requestId);
                  const decision = yield* Deferred.make<ProviderApprovalDecision>();
                  const turnId = resolveSessionCallbackTurnId(input.threadId);
                  pendingApprovals.set(requestId, { decision });
                  yield* signalSessionTurnLiveness(input.threadId, turnId);
                  yield* offerRuntimeEvent(
                    makeAcpRequestOpenedEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      requestId: runtimeRequestId,
                      permissionRequest,
                      detail:
                        permissionRequest.detail ??
                        encodeJsonStringForDiagnostics(params)?.slice(0, 2000) ??
                        "[unserializable params]",
                      args: params,
                      source: "acp.jsonrpc",
                      method: "session/request_permission",
                      rawPayload: params,
                    }),
                  );
                  const resolved = yield* Deferred.await(decision);
                  pendingApprovals.delete(requestId);
                  yield* resumeSessionTurnLiveness(input.threadId, turnId);
                  yield* offerRuntimeEvent(
                    makeAcpRequestResolvedEvent({
                      stamp: yield* makeEventStamp(),
                      provider: PROVIDER,
                      threadId: input.threadId,
                      turnId,
                      requestId: runtimeRequestId,
                      permissionRequest,
                      decision: resolved,
                    }),
                  );
                  const selectedOptionId =
                    resolved === "cancel" ? undefined : selectPermissionOptionId(params, resolved);
                  if (
                    resolved === "acceptForSession" &&
                    selectedOptionId &&
                    approvalKey !== undefined
                  ) {
                    sessionApprovedOperations.add(approvalKey);
                  }
                  return {
                    outcome: selectedOptionId
                      ? {
                          outcome: "selected" as const,
                          optionId: selectedOptionId,
                        }
                      : ({ outcome: "cancelled" } as const),
                  };
                }),
              ),
            );
            return yield* acp.start();
          }).pipe(
            Effect.mapError((error) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
            ),
          );

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            threadId: input.threadId,
            resumeCursor: {
              schemaVersion: KIMI_RESUME_VERSION,
              sessionId: started.sessionId,
            },
            createdAt: now,
            updatedAt: now,
          };

          const ctx: KimiSessionContext = {
            threadId: input.threadId,
            acpSessionId: started.sessionId,
            session,
            scope: sessionScope,
            acp,
            notificationFiber: undefined,
            pendingApprovals,
            pendingUserInputs,
            turns: [],
            lastPlanFingerprint: undefined,
            activeTurnId: undefined,
            interruptedTurnIds: new Set(),
            promptsInFlight: 0,
            promptEpoch: 0,
            discardBeforeEpoch: 0,
            promptLifecycle: yield* Semaphore.make(1),
            livenessSignals: yield* Queue.sliding<KimiTurnLivenessSignal>(1),
            livenessTurnId: undefined,
            lastTurnActivityAtNanos: undefined,
            activeToolCallIds: new Set(),
            livenessUpdatesInFlight: 0,
            promptResponsesReady: 0,
            currentModelId: undefined,
            stopped: false,
          };

          const nf = yield* Stream.runDrain(
            Stream.mapEffect(acp.getEvents(), (event) =>
              Effect.gen(function* () {
                if (event._tag === "EventStreamBarrier") {
                  yield* Deferred.succeed(event.acknowledge, undefined);
                  return;
                }
                if (
                  event._tag === "PlanUpdated" ||
                  event._tag === "ToolCallUpdated" ||
                  event._tag === "ContentDelta"
                ) {
                  yield* logNative(ctx.threadId, "session/update", event.rawPayload);
                }

                if (event._tag === "ModeChanged") return;

                const notificationTurnId = resolveNotificationTurnId(ctx);
                if (
                  notificationTurnId === undefined ||
                  ctx.interruptedTurnIds.has(notificationTurnId)
                ) {
                  return;
                }
                if (
                  event._tag === "AssistantItemStarted" ||
                  event._tag === "AssistantItemCompleted" ||
                  event._tag === "PlanUpdated" ||
                  event._tag === "ToolCallUpdated" ||
                  event._tag === "ContentDelta"
                ) {
                  yield* recordTurnActivity(ctx, notificationTurnId, event);
                }
                const stamp = yield* makeEventStamp();

                switch (event._tag) {
                  case "AssistantItemStarted":
                    yield* offerRuntimeEvent(
                      makeAcpAssistantItemEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        itemId: event.itemId,
                        lifecycle: "item.started",
                      }),
                    );
                    return;
                  case "AssistantItemCompleted":
                    yield* offerRuntimeEvent(
                      makeAcpAssistantItemEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        itemId: event.itemId,
                        lifecycle: "item.completed",
                      }),
                    );
                    return;
                  case "PlanUpdated":
                    yield* offerRuntimeEvent(
                      makeAcpPlanUpdatedEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        payload: event.payload,
                        source: "acp.jsonrpc",
                        method: "session/update",
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                  case "ToolCallUpdated":
                    yield* offerRuntimeEvent(
                      makeAcpToolCallEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        toolCall: event.toolCall,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                  case "ContentDelta":
                    yield* offerRuntimeEvent(
                      makeAcpContentDeltaEvent({
                        stamp,
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        turnId: notificationTurnId,
                        ...(event.itemId ? { itemId: event.itemId } : {}),
                        text: event.text,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                }
              }),
            ),
          ).pipe(
            Effect.catch((cause) =>
              Effect.logError("Failed to process Kimi runtime notification.", { cause }),
            ),
            Effect.forkIn(ctx.scope),
          );

          ctx.notificationFiber = nf;
          sessions.set(input.threadId, ctx);
          yield* runTurnLivenessWatchdog(ctx).pipe(Effect.forkIn(ctx.scope), Effect.asVoid);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: started.initializeResult },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Kimi ACP session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: started.sessionId },
          });

          return session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: KimiAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const prepared = yield* withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(input.threadId);
            const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
            const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
            ctx.promptsInFlight += 1;
            ctx.promptEpoch += 1;
            const promptEpoch = ctx.promptEpoch;
            ctx.activeTurnId = turnId;
            ctx.session = {
              ...ctx.session,
              status: steeringTurnId === undefined ? "connecting" : "running",
              activeTurnId: turnId,
              updatedAt: yield* nowIso,
            };

            return yield* Effect.gen(function* () {
              const text = input.input?.trim();
              const imagePromptParts = yield* Effect.forEach(
                (input.attachments ?? []).filter((attachment) => attachment.type === "image"),
                (attachment) =>
                  Effect.gen(function* () {
                    const attachmentPath = resolveAttachmentPath({
                      attachmentsDir: serverConfig.attachmentsDir,
                      attachment,
                    });
                    if (!attachmentPath) {
                      return yield* new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "session/prompt",
                        detail: `Invalid attachment id '${attachment.id}'.`,
                      });
                    }
                    const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                      Effect.mapError(
                        (cause) =>
                          new ProviderAdapterRequestError({
                            provider: PROVIDER,
                            method: "session/prompt",
                            detail: cause.message,
                            cause,
                          }),
                      ),
                    );
                    return {
                      type: "image",
                      data: Buffer.from(bytes).toString("base64"),
                      mimeType: attachment.mimeType,
                    } satisfies EffectAcpSchema.ContentBlock;
                  }),
              );
              const promptParts: Array<EffectAcpSchema.ContentBlock> = [
                ...(text ? [{ type: "text" as const, text }] : []),
                ...imagePromptParts,
              ];

              if (promptParts.length === 0) {
                return yield* new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "sendTurn",
                  issue: "Turn requires non-empty text or attachments.",
                });
              }

              const runtimeInstructions = buildRuntimeInstructions({
                harness: "Kimi",
                model: undefined,
              });
              for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
                yield* Effect.yieldNow;
              }
              if (ctx.interruptedTurnIds.has(turnId)) {
                yield* settlePromptInFlight(input.threadId, turnId, ctx.acpSessionId, {
                  completedStopReason: "cancelled",
                  emitTurnCompletion: false,
                  settleAllPrompts: true,
                });
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: "Kimi prompt was interrupted during preparation.",
                });
              }
              if (steeringTurnId === undefined) {
                ctx.lastPlanFingerprint = undefined;
              }
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: turnId,
                updatedAt: yield* nowIso,
              };
              if (steeringTurnId === undefined) {
                yield* beginTurnLiveness(ctx, turnId);
              } else {
                yield* refreshSessionTurnLiveness(input.threadId, turnId);
              }

              if (steeringTurnId === undefined) {
                yield* offerRuntimeEvent({
                  type: "turn.started",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: {},
                });
              } else {
                yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
                yield* settlePendingUserInputsAsCancelled(ctx.pendingUserInputs);
                ctx.discardBeforeEpoch = promptEpoch;
              }

              return {
                acp: ctx.acp,
                acpSessionId: ctx.acpSessionId,
                promptParts,
                runtimeInstructions,
                turnId,
                promptEpoch,
                promptLifecycle: ctx.promptLifecycle,
                steeringTurnId,
              };
            }).pipe(
              Effect.tapCause(() =>
                Effect.gen(function* () {
                  const liveCtx = sessions.get(input.threadId);
                  if (!liveCtx) return;
                  yield* settlePromptInFlight(input.threadId, turnId, liveCtx.acpSessionId, {
                    errorMessage: "Kimi prompt preparation failed.",
                    emitTurnCompletion: false,
                  });
                }),
              ),
            );
          }),
        );
        const promptSettled = yield* Ref.make(false);
        const promptRpcSucceeded = yield* Ref.make(false);
        const promptResultRef = yield* Ref.make<EffectAcpSchema.PromptResponse | undefined>(
          undefined,
        );
        const promptFailureMessageRef = yield* Ref.make<string | undefined>(undefined);

        return yield* Effect.gen(function* () {
          const promptStart = yield* prepared.promptLifecycle.withPermit(
            Effect.gen(function* () {
              const liveCtx = sessions.get(input.threadId);
              const interrupted = liveCtx?.interruptedTurnIds.has(prepared.turnId) === true;
              if (
                !liveCtx ||
                liveCtx.acpSessionId !== prepared.acpSessionId ||
                prepared.promptEpoch < liveCtx.discardBeforeEpoch ||
                interrupted
              ) {
                return { _tag: "Skipped" as const, interrupted };
              }
              if (prepared.steeringTurnId !== undefined) {
                yield* Effect.ignore(
                  liveCtx.acp.cancel.pipe(
                    Effect.mapError((error) =>
                      mapAcpToAdapterError(PROVIDER, input.threadId, "session/cancel", error),
                    ),
                  ),
                );
              }
              if (liveCtx.interruptedTurnIds.has(prepared.turnId)) {
                return { _tag: "Skipped" as const, interrupted: true };
              }
              const dispatched = yield* Deferred.make<void>();
              const fiber = yield* liveCtx.acp
                .prompt(
                  {
                    prompt: [
                      ...prepared.promptParts,
                      { type: "text", text: prepared.runtimeInstructions },
                    ],
                  },
                  { dispatched },
                )
                .pipe(Effect.forkChild({ startImmediately: true }));
              yield* Effect.raceFirst(
                Deferred.await(dispatched),
                Fiber.await(fiber).pipe(Effect.asVoid),
              );
              return { _tag: "Started" as const, fiber };
            }),
          );
          if (promptStart._tag === "Skipped") {
            yield* withThreadLock(
              input.threadId,
              settlePromptInFlight(
                input.threadId,
                prepared.turnId,
                prepared.acpSessionId,
                promptStart.interrupted
                  ? { completedStopReason: "cancelled", settleAllPrompts: true }
                  : { emitTurnCompletion: false },
              ),
            );
            yield* Ref.set(promptSettled, true);
            const liveCtx = sessions.get(input.threadId);
            return {
              threadId: input.threadId,
              turnId: prepared.turnId,
              resumeCursor: liveCtx?.session.resumeCursor,
            };
          }

          const result = yield* Fiber.join(promptStart.fiber).pipe(
            Effect.tap((promptResult) =>
              Effect.all(
                [
                  Ref.set(promptRpcSucceeded, true),
                  Ref.set(promptResultRef, promptResult),
                  markPromptResponseReady(input.threadId, prepared.acpSessionId, prepared.turnId),
                ],
                { discard: true },
              ),
            ),
            Effect.tapError((error) =>
              Ref.set(
                promptFailureMessageRef,
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error).message,
              ).pipe(Effect.andThen(prepared.acp.drainEvents)),
            ),
            Effect.mapError((error) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
            ),
          );

          return yield* withThreadLock(
            input.threadId,
            Effect.gen(function* () {
              const ctx = yield* requireSession(input.threadId);
              if (ctx.acpSessionId !== prepared.acpSessionId) {
                yield* settlePromptInFlight(
                  input.threadId,
                  prepared.turnId,
                  prepared.acpSessionId,
                  {
                    errorMessage: "Kimi session changed before the turn completed.",
                    settleAllPrompts: true,
                  },
                );
                yield* Ref.set(promptSettled, true);
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: "Kimi session changed before the turn completed.",
                });
              }
              for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
                yield* Effect.yieldNow;
              }
              yield* prepared.acp.drainEvents;
              consumePromptResponseReady(ctx);
              if (ctx.interruptedTurnIds.has(prepared.turnId)) {
                yield* Ref.set(promptSettled, true);
                return {
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  resumeCursor: ctx.session.resumeCursor,
                };
              }

              if (
                ctx.promptsInFlight <= 0 ||
                ctx.activeTurnId !== prepared.turnId ||
                ctx.session.activeTurnId !== prepared.turnId
              ) {
                yield* Ref.set(promptSettled, true);
                return {
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  resumeCursor: ctx.session.resumeCursor,
                };
              }

              appendPromptResultToTurn(ctx, prepared.turnId, prepared.promptParts, result);
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: prepared.turnId,
                updatedAt: yield* nowIso,
              };
              const remainingPrompts = Math.max(0, ctx.promptsInFlight - 1);
              ctx.promptsInFlight = remainingPrompts;

              if (
                remainingPrompts === 0 &&
                ctx.activeTurnId === prepared.turnId &&
                ctx.session.activeTurnId === prepared.turnId
              ) {
                if (ctx.interruptedTurnIds.has(prepared.turnId)) {
                  yield* Ref.set(promptSettled, true);
                  return {
                    threadId: input.threadId,
                    turnId: prepared.turnId,
                    resumeCursor: ctx.session.resumeCursor,
                  };
                }
                const completedAt = yield* nowIso;
                const { activeTurnId: _completedTurnId, ...readySession } = ctx.session;
                ctx.activeTurnId = undefined;
                ctx.session = { ...readySession, status: "ready", updatedAt: completedAt };
                yield* clearTurnLiveness(ctx);
                yield* offerRuntimeEvent({
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  payload: {
                    state: result.stopReason === "cancelled" ? "cancelled" : "completed",
                    stopReason: result.stopReason ?? null,
                  },
                });
                ctx.interruptedTurnIds.delete(prepared.turnId);
                yield* Ref.set(promptSettled, true);
              } else if (remainingPrompts > 0) {
                yield* Ref.set(promptSettled, true);
              }

              return {
                threadId: input.threadId,
                turnId: prepared.turnId,
                resumeCursor: ctx.session.resumeCursor,
              };
            }),
          );
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              if (yield* Ref.get(promptSettled)) return;

              if (yield* Ref.get(promptRpcSucceeded)) {
                const promptResult = yield* Ref.get(promptResultRef);
                if (promptResult === undefined) return;
                yield* withThreadLock(
                  input.threadId,
                  Effect.gen(function* () {
                    const ctx = yield* requireSession(input.threadId);
                    if (ctx.acpSessionId !== prepared.acpSessionId) {
                      yield* settlePromptInFlight(
                        input.threadId,
                        prepared.turnId,
                        prepared.acpSessionId,
                        {
                          errorMessage: "Kimi session changed before the turn completed.",
                          settleAllPrompts: true,
                        },
                      );
                      return;
                    }
                    if (ctx.interruptedTurnIds.has(prepared.turnId)) return;
                    consumePromptResponseReady(ctx);
                    if (
                      ctx.promptsInFlight <= 0 ||
                      ctx.activeTurnId !== prepared.turnId ||
                      ctx.session.activeTurnId !== prepared.turnId
                    ) {
                      return;
                    }
                    appendPromptResultToTurn(
                      ctx,
                      prepared.turnId,
                      prepared.promptParts,
                      promptResult,
                    );
                    yield* settlePromptInFlight(
                      input.threadId,
                      prepared.turnId,
                      prepared.acpSessionId,
                      { completedStopReason: promptResult.stopReason ?? null },
                    );
                  }),
                );
                return;
              }

              const errorMessage = yield* Ref.get(promptFailureMessageRef);
              yield* withThreadLock(
                input.threadId,
                settlePromptInFlight(input.threadId, prepared.turnId, prepared.acpSessionId, {
                  errorMessage: errorMessage ?? "Kimi prompt request failed.",
                }),
              );
            }).pipe(Effect.catch(() => Effect.void)),
          ),
        );
      });

    const interruptTurn: KimiAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        const observed = yield* Effect.sync(() => {
          const ctx = sessions.get(threadId);
          if (!ctx || ctx.stopped) {
            return { _tag: "Proceed" as const, acpSessionId: undefined, interruptedTurnId: turnId };
          }
          const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
          if (turnId !== undefined && activeTurnId !== undefined && activeTurnId !== turnId) {
            return { _tag: "Ignore" as const };
          }
          const interruptedTurnId = turnId ?? activeTurnId;
          if (interruptedTurnId !== undefined) {
            ctx.interruptedTurnIds.add(interruptedTurnId);
          }
          return { _tag: "Proceed" as const, acpSessionId: ctx.acpSessionId, interruptedTurnId };
        });
        if (observed._tag === "Ignore") return;

        yield* withThreadLock(
          threadId,
          Effect.gen(function* () {
            const ctx = yield* requireSession(threadId);
            if (observed.acpSessionId !== undefined && ctx.acpSessionId !== observed.acpSessionId) {
              return;
            }
            const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
            if (turnId !== undefined && activeTurnId !== undefined && activeTurnId !== turnId) {
              return;
            }
            const interruptedTurnId =
              observed.interruptedTurnId ?? turnId ?? activeTurnId ?? ctx.session.activeTurnId;
            yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
            yield* settlePendingUserInputsAsCancelled(ctx.pendingUserInputs);
            yield* Effect.ignore(
              ctx.acp.cancel.pipe(
                Effect.mapError((error) =>
                  mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
                ),
              ),
            );
            if (interruptedTurnId) {
              ctx.interruptedTurnIds.add(interruptedTurnId);
              yield* settlePromptInFlight(threadId, interruptedTurnId, ctx.acpSessionId, {
                completedStopReason: "cancelled",
                settleAllPrompts: true,
              });
            } else if (
              ctx.promptsInFlight > 0 ||
              ctx.session.status === "running" ||
              ctx.session.status === "connecting"
            ) {
              const updatedAt = yield* nowIso;
              ctx.promptsInFlight = 0;
              ctx.activeTurnId = undefined;
              const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
              ctx.session = { ...readySession, status: "ready", updatedAt };
              yield* clearTurnLiveness(ctx);
            }
          }),
        );
      });

    const respondToRequest: KimiAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: KimiAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingUserInputs.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "user-input",
            detail: `Unknown pending user-input request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.resolution, { _tag: "answered", answers });
      });

    const readThread: KimiAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: KimiAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "Kimi ACP sessions do not support provider-side rollback yet.",
        });
      });

    const stopSession: KimiAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: KimiAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: KimiAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: KimiAdapterShape["stopAll"] = () =>
      Effect.forEach(Array.from(sessions.values()), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.ignore(stopAll()).pipe(
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
        Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported" },
      compaction: { type: "slash-command", command: "/compact" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies KimiAdapterShape;
  });
}
