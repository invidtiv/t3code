import { type DeepSeekSettings, ProviderDriverKind, type RuntimeMode } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import { normalizeModelSlug } from "@t3tools/shared/model";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const DEEPSEEK_DRIVER_KIND = ProviderDriverKind.make("deepseek");

type DeepSeekAcpRuntimeSettings = Pick<DeepSeekSettings, "binaryPath">;

interface DeepSeekAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "clientCapabilities" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly deepSeekSettings: DeepSeekAcpRuntimeSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
  readonly runtimeMode?: RuntimeMode;
}

export function buildDeepSeekAcpSpawnInput(
  deepSeekSettings: DeepSeekAcpRuntimeSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  _runtimeMode?: RuntimeMode,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: deepSeekSettings?.binaryPath || "dsh",
    args: ["--profile", "acp"],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export const makeDeepSeekAcpRuntime = (
  input: DeepSeekAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildDeepSeekAcpSpawnInput(
          input.deepSeekSettings,
          input.cwd,
          input.environment,
          input.runtimeMode,
        ),
        authMethodId: "login",
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });

export const DEEPSEEK_DEFAULT_MODEL_SLUG = "deepseek-r2";

export function resolveDeepSeekAcpBaseModelId(model: string | null | undefined): string {
  const trimmed = model?.trim();
  const base = trimmed && trimmed.length > 0 ? trimmed : DEEPSEEK_DEFAULT_MODEL_SLUG;
  return normalizeModelSlug(base, DEEPSEEK_DRIVER_KIND) ?? DEEPSEEK_DEFAULT_MODEL_SLUG;
}
