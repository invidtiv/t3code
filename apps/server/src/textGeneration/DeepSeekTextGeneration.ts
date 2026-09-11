import * as Effect from "effect/Effect";

import { type DeepSeekSettings, TextGenerationError } from "@t3tools/contracts";
import type * as TextGeneration from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  new TextGenerationError({
    operation,
    detail: "DeepSeek text generation is not yet supported.",
  });

export const makeDeepSeekTextGeneration = Effect.fn("makeDeepSeekTextGeneration")(function* (
  _deepSeekSettings: DeepSeekSettings,
  _environment: NodeJS.ProcessEnv = process.env,
) {
  return {
    generateCommitMessage: (_input) => Effect.fail(unsupported("generateCommitMessage")),
    generatePrContent: (_input) => Effect.fail(unsupported("generatePrContent")),
    generateBranchName: (_input) => Effect.fail(unsupported("generateBranchName")),
    generateThreadTitle: (_input) => Effect.fail(unsupported("generateThreadTitle")),
  } satisfies TextGeneration.TextGeneration["Service"];
});
