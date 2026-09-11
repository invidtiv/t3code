import {
  type DeepSeekSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderAuth,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type * as EffectAcpSchema from "effect-acp/schema";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  DEEPSEEK_DEFAULT_MODEL_SLUG,
  makeDeepSeekAcpRuntime,
  resolveDeepSeekAcpBaseModelId,
} from "../acp/DeepSeekAcpSupport.ts";
import { sessionModelStateFromInitialize } from "../acp/AcpRuntimeModel.ts";

const DEEPSEEK_PRESENTATION = {
  displayName: "DeepSeek",
  showInteractionModeToggle: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const DEEPSEEK_ACP_INITIALIZE_TIMEOUT_MS = 8_000;

const DEEPSEEK_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: DEEPSEEK_DEFAULT_MODEL_SLUG,
    name: "DeepSeek K3",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

export function buildInitialDeepSeekProviderSnapshot(
  deepSeekSettings: DeepSeekSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = deepSeekModelsFromSettings(deepSeekSettings.customModels);

    if (!deepSeekSettings.enabled) {
      return buildServerProvider({
        presentation: DEEPSEEK_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "DeepSeek is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: DEEPSEEK_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking DeepSeek CLI availability...",
      },
    });
  });
}

function deepSeekModelsFromSettings(
  customModels: ReadonlyArray<import("@t3tools/contracts").CustomModelSetting> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = DEEPSEEK_BUILT_IN_MODELS,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

export function buildDeepSeekModelsFromSessionModelState(
  modelState: EffectAcpSchema.SessionModelState | null | undefined,
): ReadonlyArray<ServerProviderModel> {
  if (!modelState || modelState.availableModels.length === 0) {
    return [];
  }
  const currentModelId = modelState.currentModelId.trim();
  const seen = new Set<string>();
  return modelState.availableModels.flatMap((model): ServerProviderModel[] => {
    const slug = resolveDeepSeekAcpBaseModelId(model.modelId);
    if (!slug || seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: model.name.trim() || slug,
        isCustom: false,
        ...(model.modelId.trim() === currentModelId ? { isDefault: true } : {}),
        capabilities: EMPTY_CAPABILITIES,
      },
    ];
  });
}

const runDeepSeekCliCommand = (
  deepSeekSettings: DeepSeekSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = deepSeekSettings.binaryPath || "dsh";
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

const discoverDeepSeekModelsViaAcpInitialize = (
  deepSeekSettings: DeepSeekSettings,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const acp = yield* makeDeepSeekAcpRuntime({
      deepSeekSettings,
      environment,
      childProcessSpawner,
      cwd: process.cwd(),
      clientInfo: { name: "t3-code-provider-probe", version: "0.0.0" },
    });
    const initialized = yield* acp.initialize();
    return buildDeepSeekModelsFromSessionModelState(sessionModelStateFromInitialize(initialized));
  }).pipe(Effect.scoped);

export const checkDeepSeekProviderStatus = Effect.fn("checkDeepSeekProviderStatus")(function* (
  deepSeekSettings: DeepSeekSettings,
  environment: NodeJS.ProcessEnv = process.env,
  _cwd?: string,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = deepSeekModelsFromSettings(deepSeekSettings.customModels);

  if (!deepSeekSettings.enabled) {
    return buildServerProvider({
      presentation: DEEPSEEK_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "DeepSeek is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runDeepSeekCliCommand(
    deepSeekSettings,
    ["--version"],
    environment,
  ).pipe(Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS), Effect.result);

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("DeepSeek CLI health check failed.", { errorTag: error._tag });
    return buildServerProvider({
      presentation: DEEPSEEK_PRESENTATION,
      enabled: deepSeekSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "DeepSeek CLI (`dsh`) is not installed or not on PATH."
          : "Failed to execute DeepSeek CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: DEEPSEEK_PRESENTATION,
      enabled: deepSeekSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "DeepSeek CLI is installed but timed out while running `dsh --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("DeepSeek CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return buildServerProvider({
      presentation: DEEPSEEK_PRESENTATION,
      enabled: deepSeekSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "DeepSeek CLI is installed but failed to run.",
      },
    });
  }

  const auth: ServerProviderAuth = { status: "unknown" };

  const acpExit = yield* discoverDeepSeekModelsViaAcpInitialize(deepSeekSettings, environment).pipe(
    Effect.timeoutOption(DEEPSEEK_ACP_INITIALIZE_TIMEOUT_MS),
    Effect.exit,
  );
  const acpModels = Exit.isSuccess(acpExit) ? Option.getOrElse(acpExit.value, () => []) : [];
  const acpFailed = Exit.isFailure(acpExit) || Option.isNone(acpExit.value);
  if (acpFailed) {
    yield* Effect.logWarning("DeepSeek ACP initialize probe failed or timed out.", {
      errorTag: Exit.isFailure(acpExit) ? causeErrorTag(acpExit.cause) : "Timeout",
    });
  }

  const models =
    acpModels.length > 0
      ? deepSeekModelsFromSettings(deepSeekSettings.customModels, acpModels)
      : fallbackModels;

  return buildServerProvider({
    presentation: DEEPSEEK_PRESENTATION,
    enabled: deepSeekSettings.enabled,
    checkedAt,
    models,
    slashCommands: [COMPACT_SLASH_COMMAND],
    probe: {
      installed: true,
      version,
      status: acpFailed ? "warning" : "ready",
      auth,
      ...(acpFailed
        ? {
            message:
              "DeepSeek CLI is installed but ACP initialize failed. Model options may be incomplete.",
          }
        : {}),
    },
  });
});
