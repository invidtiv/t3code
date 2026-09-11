import { DeepSeekSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeDeepSeekTextGeneration } from "../../textGeneration/DeepSeekTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeDeepSeekAdapter } from "../Layers/DeepSeekAdapter.ts";
import {
  buildInitialDeepSeekProviderSnapshot,
  checkDeepSeekProviderStatus,
} from "../Layers/DeepSeekProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
const decodeDeepSeekSettings = Schema.decodeSync(DeepSeekSettings);

const DRIVER_KIND = ProviderDriverKind.make("deepseek");
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type DeepSeekDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

export const DeepSeekDriver: ProviderDriver<DeepSeekSettings, DeepSeekDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "DeepSeek",
    supportsMultipleInstances: true,
  },
  configSchema: DeepSeekSettings,
  defaultConfig: (): DeepSeekSettings => decodeDeepSeekSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverSettings = yield* ServerSettingsService;
      const { cwd } = yield* ServerConfig;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies DeepSeekSettings;
      const adapter = yield* makeDeepSeekAdapter(effectiveConfig, {
        environment: processEnv,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
        instanceId,
      });
      const textGeneration = yield* makeDeepSeekTextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkDeepSeekProviderStatus(effectiveConfig, processEnv, cwd).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DeepSeekSettings>>(
        {
          resolveMaintenance: () => Effect.succeed(MAINTENANCE_CAPABILITIES),
          getSettings: snapshotSettings.getSettings,
          streamSettings: snapshotSettings.streamSettings,
          haveSettingsChanged: haveProviderSnapshotSettingsChanged,
          initialSnapshot: (settings) =>
            buildInitialDeepSeekProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
          checkProvider,
        },
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build DeepSeek snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        snapshotForCwd: () => snapshot.getSnapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
