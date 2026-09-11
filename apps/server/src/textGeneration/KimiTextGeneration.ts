import * as Effect from "effect/Effect";

import { type KimiSettings, TextGenerationError } from "@t3tools/contracts";
import type * as TextGeneration from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  new TextGenerationError({
    operation,
    detail: "Kimi text generation is not yet supported.",
  });

export const makeKimiTextGeneration = Effect.fn("makeKimiTextGeneration")(function* (
  _kimiSettings: KimiSettings,
  _environment: NodeJS.ProcessEnv = process.env,
) {
  return {
    generateCommitMessage: (_input) => Effect.fail(unsupported("generateCommitMessage")),
    generatePrContent: (_input) => Effect.fail(unsupported("generatePrContent")),
    generateBranchName: (_input) => Effect.fail(unsupported("generateBranchName")),
    generateThreadTitle: (_input) => Effect.fail(unsupported("generateThreadTitle")),
  } satisfies TextGeneration.TextGeneration["Service"];
});
