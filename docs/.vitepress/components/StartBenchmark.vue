<script setup lang="ts">
import { computed, ref } from "vue";

type BenchmarkType = "property" | "optimization";

const REPO_OWNER = "Recon-Fuzz";
const REPO_NAME = "scfuzzbench";
const NEW_ISSUE_URL = `https://github.com/${REPO_OWNER}/${REPO_NAME}/issues/new`;

// Defaults are intentionally aligned with the repo's typical local `.env` values.
// Avoid putting anything secret here: this is a fully static site.
const targetRepoUrl = ref("https://github.com/Recon-Fuzz/aave-v4-scfuzzbench");
const targetCommit = ref("v0.5.6-recon");

const benchmarkType = ref<BenchmarkType>("property");
const instanceType = ref("c6a.4xlarge");
const instancesPerFuzzer = ref(4);
const timeoutHours = ref(1);

// Advanced / optional overrides.
const foundryVersion = ref("");
const foundryGitRepo = ref("https://github.com/aviggiano/foundry");
const foundryGitRef = ref("master");

const echidnaVersion = ref("");
const medusaVersion = ref("");
const bitwuzlaVersion = ref("");

const gitTokenSsmParameterName = ref("/scfuzzbench/recon/github_token");

const propertiesPath = ref("");
const fuzzerEnvJson = ref("");

const requestJson = computed(() => {
  const payload: Record<string, unknown> = {
    target_repo_url: targetRepoUrl.value.trim(),
    target_commit: targetCommit.value.trim(),
    benchmark_type: benchmarkType.value,
    instance_type: instanceType.value.trim(),
    instances_per_fuzzer: instancesPerFuzzer.value,
    timeout_hours: timeoutHours.value,

    foundry_version: foundryVersion.value.trim(),
    foundry_git_repo: foundryGitRepo.value.trim(),
    foundry_git_ref: foundryGitRef.value.trim(),

    echidna_version: echidnaVersion.value.trim(),
    medusa_version: medusaVersion.value.trim(),
    bitwuzla_version: bitwuzlaVersion.value.trim(),

    git_token_ssm_parameter_name: gitTokenSsmParameterName.value.trim(),

    properties_path: propertiesPath.value.trim(),
    fuzzer_env_json: fuzzerEnvJson.value.trim(),
  };

  return JSON.stringify(payload, null, 2);
});

const issueTitle = computed(() => {
  const repo = targetRepoUrl.value.trim().replace(/^https?:\/\//, "");
  const refPart = targetCommit.value.trim() ? `@${targetCommit.value.trim()}` : "";
  const typePart = benchmarkType.value ? ` (${benchmarkType.value})` : "";
  return `benchmark: ${repo}${refPart}${typePart}`;
});

const issueBody = computed(() => {
  return [
    "<!-- scfuzzbench-benchmark-request:v1 -->",
    "",
    "This issue was generated from https://scfuzzbench.com/start.",
    "",
    "A maintainer must change the label from `benchmark/needs-approval` to `benchmark/approved` to start the run.",
    "",
    "```json",
    requestJson.value,
    "```",
    "",
    "Notes:",
    "- Do not include secrets in this issue.",
    "- `target_commit` may be a commit SHA, tag, or branch name.",
  ].join("\n");
});

const issueUrl = computed(() => {
  const params = new URLSearchParams();
  params.set("title", issueTitle.value);
  params.set("body", issueBody.value);
  // These labels must exist in the repo to be pre-applied; the workflow will also ensure them.
  params.set("labels", "benchmark/needs-approval");
  return `${NEW_ISSUE_URL}?${params.toString()}`;
});

const showAdvanced = ref(false);
</script>

<template>
  <div class="sb-start">
    <div class="sb-start__panel">
      <div class="sb-start__grid">
        <label class="sb-start__field">
          <div class="sb-start__label">Target repo URL</div>
          <input v-model="targetRepoUrl" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Target commit / tag / branch</div>
          <input v-model="targetCommit" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Benchmark type</div>
          <select v-model="benchmarkType" class="sb-start__input">
            <option value="property">property</option>
            <option value="optimization">optimization</option>
          </select>
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">EC2 instance type</div>
          <input v-model="instanceType" class="sb-start__input" type="text" />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Instances per fuzzer (1 to 20)</div>
          <input
            v-model.number="instancesPerFuzzer"
            class="sb-start__input"
            type="number"
            min="1"
            max="20"
            step="1"
          />
        </label>

        <label class="sb-start__field">
          <div class="sb-start__label">Timeout (hours, 0.25 to 72)</div>
          <input
            v-model.number="timeoutHours"
            class="sb-start__input"
            type="number"
            min="0.25"
            max="72"
            step="0.25"
          />
        </label>
      </div>

      <div class="sb-start__actions">
        <a class="sb-start__button" :href="issueUrl" target="_blank" rel="noreferrer">
          Open GitHub request issue
        </a>

        <button class="sb-start__button sb-start__button--ghost" type="button" @click="showAdvanced = !showAdvanced">
          {{ showAdvanced ? "Hide advanced" : "Show advanced" }}
        </button>
      </div>

      <div v-if="showAdvanced" class="sb-start__advanced">
        <div class="sb-start__grid">
          <label class="sb-start__field">
            <div class="sb-start__label">GitHub token SSM parameter name (for private repos)</div>
            <input v-model="gitTokenSsmParameterName" class="sb-start__input" type="text" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Foundry version override (optional)</div>
            <input v-model="foundryVersion" class="sb-start__input" type="text" placeholder="e.g. v1.6.0-rc1" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Foundry git repo (build from source, optional)</div>
            <input v-model="foundryGitRepo" class="sb-start__input" type="text" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Foundry git ref (optional)</div>
            <input v-model="foundryGitRef" class="sb-start__input" type="text" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Echidna version override (optional)</div>
            <input v-model="echidnaVersion" class="sb-start__input" type="text" placeholder="e.g. 2.3.1" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Medusa version override (optional)</div>
            <input v-model="medusaVersion" class="sb-start__input" type="text" placeholder="e.g. 1.4.1" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Bitwuzla version override (optional)</div>
            <input v-model="bitwuzlaVersion" class="sb-start__input" type="text" placeholder="e.g. 0.8.2" />
          </label>

          <label class="sb-start__field">
            <div class="sb-start__label">Properties path (optional)</div>
            <input v-model="propertiesPath" class="sb-start__input" type="text" placeholder="repo-relative path" />
          </label>

          <label class="sb-start__field sb-start__field--full">
            <div class="sb-start__label">Extra fuzzer env JSON (optional)</div>
            <textarea
              v-model="fuzzerEnvJson"
              class="sb-start__input sb-start__textarea"
              rows="6"
              placeholder="{\"SCFUZZBENCH_PROPERTIES_PATH\":\"...\"}"
            />
          </label>
        </div>

        <p class="sb-start__hint">
          Note: setting <code>properties_path</code> or <code>fuzzer_env_json</code> causes the workflow to pass a
          complete <code>fuzzer_env</code> map to Terraform (overriding its defaults). Leave these blank unless you know
          you want that.
        </p>
      </div>
    </div>

    <details class="sb-start__preview" open>
      <summary>Request JSON preview</summary>
      <pre><code>{{ requestJson }}</code></pre>
    </details>
  </div>
</template>
