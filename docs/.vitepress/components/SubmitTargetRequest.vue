<script setup lang="ts">
import { computed, ref } from "vue";

type BenchmarkType = "property" | "optimization";

const REPO_OWNER = "Recon-Fuzz";
const REPO_NAME = "scfuzzbench";
const NEW_ISSUE_URL = `https://github.com/${REPO_OWNER}/${REPO_NAME}/issues/new`;
const TARGETS_DOC_URL = "https://scfuzzbench.com/targets";
const TARGETS_DOC_SOURCE_URL =
  "https://github.com/Recon-Fuzz/scfuzzbench/blob/main/docs/targets.md";

const upstreamTargetRepoUrl = ref("https://github.com/superform-xyz/v2-periphery");
const vulnerableBaselineCommitShaForDev = ref("");
const reconHarnessSourceRepoUrl = ref("https://github.com/superform-xyz/v2-periphery");
const reconHarnessSourceRefForTestRecon = ref("dev");
const destinationRepoUrl = ref("https://github.com/Recon-Fuzz/superform-v2-periphery-scfuzzbench");
const baseBranchName = ref("dev");
const reconBranchName = ref("dev-recon");
const benchmarkType = ref<BenchmarkType>("property");
const notes = ref("");

function compactRepoLabel(raw: string): string {
  return raw
    .trim()
    .replace(/^https?:\/\//, "")
    .replace(/^github\.com\//, "")
    .replace(/\.git\/?$/, "")
    .replace(/\/+$/, "");
}

const requestPayload = computed(() => {
  return {
    upstream_target_repo_url: upstreamTargetRepoUrl.value.trim(),
    vulnerable_baseline_commit_sha_for_dev:
      vulnerableBaselineCommitShaForDev.value.trim(),
    recon_harness_source_repo_url: reconHarnessSourceRepoUrl.value.trim(),
    recon_harness_source_ref_for_test_recon:
      reconHarnessSourceRefForTestRecon.value.trim(),
    destination_repo_url: destinationRepoUrl.value.trim(),
    base_branch_name: baseBranchName.value.trim(),
    recon_branch_name: reconBranchName.value.trim(),
    benchmark_type: benchmarkType.value,
  };
});

const requestJson = computed(() => JSON.stringify(requestPayload.value, null, 2));

const agentHandoffPrompt = computed(() => {
  const r = requestPayload.value;
  return [
    "Read docs/targets.md and execute it end-to-end for the following target.",
    "",
    "Inputs:",
    `- upstream_target_repo_url: ${r.upstream_target_repo_url}`,
    `- vulnerable_baseline_commit_sha_for_dev: ${r.vulnerable_baseline_commit_sha_for_dev}`,
    `- recon_harness_source_repo_url: ${r.recon_harness_source_repo_url}`,
    `- recon_harness_source_ref_for_test_recon: ${r.recon_harness_source_ref_for_test_recon}`,
    `- destination_repo_url: ${r.destination_repo_url}`,
    `- base_branch_name: ${r.base_branch_name}`,
    `- recon_branch_name: ${r.recon_branch_name}`,
    `- benchmark_type: ${r.benchmark_type}`,
  ].join("\n");
});

const issueTitle = computed(() => {
  const source = compactRepoLabel(upstreamTargetRepoUrl.value);
  const dest = compactRepoLabel(destinationRepoUrl.value);
  const sha = vulnerableBaselineCommitShaForDev.value.trim();
  const shaShort = sha ? `@${sha.slice(0, 12)}` : "";
  return `target: ${source}${shaShort} -> ${dest}`;
});

const issueBody = computed(() => {
  const trimmedNotes = notes.value.trim();
  const noteBlock = trimmedNotes ? trimmedNotes : "- (none)";

  return [
    "<!-- scfuzzbench-target-request:v1 -->",
    "",
    "This issue was generated from https://scfuzzbench.com/submit-target.",
    "",
    "Use the onboarding playbook:",
    `- ${TARGETS_DOC_URL}`,
    `- ${TARGETS_DOC_SOURCE_URL}`,
    "",
    "Requested target inputs:",
    "```json",
    requestJson.value,
    "```",
    "",
    "Agent handoff prompt:",
    "```text",
    agentHandoffPrompt.value,
    "```",
    "",
    "Requester notes:",
    noteBlock,
    "",
    "Expected deliverables:",
    "- Recon-Fuzz target repository created and pushed.",
    "- Baseline branch (usually `dev`) at vulnerable commit.",
    "- Recon branch (usually `dev-recon`) with full `test/recon` harness and config files.",
    "- PR `dev-recon -> dev` with local validation summary and `/start` JSON guidance.",
  ].join("\n");
});

const issueUrl = computed(() => {
  const params = new URLSearchParams();
  params.set("template", "target-request.md");
  params.set("title", issueTitle.value);
  params.set("body", issueBody.value);
  return `${NEW_ISSUE_URL}?${params.toString()}`;
});
</script>

<template>
  <div class="sb-start">
    <div class="sb-start__panel">
      <div class="sb-start__grid">
        <label class="sb-start__field">
          <div class="sb-start__label">Upstream target repo URL</div>
          <input v-model="upstreamTargetRepoUrl" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Vulnerable baseline commit SHA for <code>dev</code></div>
          <input
            v-model="vulnerableBaselineCommitShaForDev"
            class="sb-start__input"
            type="text"
            placeholder="40-char commit SHA"
          />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Recon harness source repo URL</div>
          <input v-model="reconHarnessSourceRepoUrl" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Recon harness source ref for <code>test/recon</code></div>
          <input
            v-model="reconHarnessSourceRefForTestRecon"
            class="sb-start__input"
            type="text"
            placeholder="branch or commit"
          />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Destination repo URL (Recon-Fuzz)</div>
          <input v-model="destinationRepoUrl" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Benchmark type</div>
          <select v-model="benchmarkType" class="sb-start__input">
            <option value="property">property</option>
            <option value="optimization">optimization</option>
          </select>
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Base branch name</div>
          <input v-model="baseBranchName" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Recon branch name</div>
          <input v-model="reconBranchName" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field sb-start__field--full">
          <div class="sb-start__label">Requester notes (optional)</div>
          <textarea
            v-model="notes"
            class="sb-start__input sb-start__textarea"
            rows="5"
            placeholder="Context, constraints, or links for the implementing agent."
          />
        </label>
      </div>

      <div class="sb-start__actions">
        <a class="sb-start__button" :href="issueUrl" target="_blank" rel="noreferrer">
          Open target request issue
        </a>
      </div>
    </div>

    <details class="sb-start__preview" open>
      <summary>Target request JSON preview</summary>
      <pre><code>{{ requestJson }}</code></pre>
    </details>

    <details class="sb-start__preview">
      <summary>Agent handoff prompt preview</summary>
      <pre><code>{{ agentHandoffPrompt }}</code></pre>
    </details>
  </div>
</template>
