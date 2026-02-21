import { defineConfig } from "vitepress";

export default defineConfig({
  title: "scfuzzbench",
  description: "Benchmark suite for smart-contract fuzzers.",

  // Custom domain, so always build with root base.
  base: "/",

  // Start in dark mode by default, but keep the toggle available.
  //
  // VitePress persists the chosen theme in localStorage; this sets an initial
  // value only when the user has not picked one yet.
  head: [
    ["meta", { name: "viewport", content: "width=device-width, initial-scale=1" }],
    ["meta", { name: "theme-color", content: "#0b1220" }],
    [
      "script",
      {},
      `;(() => {
  try {
    const key = "vitepress-theme-appearance";
    if (localStorage.getItem(key) === null) localStorage.setItem(key, "dark");
  } catch {}
})();`,
    ],
  ],

  appearance: true,

  themeConfig: {
    nav: [
      { text: "Introduction", link: "/introduction" },
      { text: "Runs", link: "/runs/" },
      { text: "Benchmarks", link: "/benchmarks/" },
      { text: "Start benchmark", link: "/start" },
      { text: "Submit target request", link: "/submit-target" },
      { text: "Targets", link: "/targets" },
      { text: "Methodology", link: "/methodology" },
      { text: "GitHub", link: "https://github.com/Recon-Fuzz/scfuzzbench" },
    ],

    sidebar: [
      {
        text: "Explore",
        items: [
          { text: "Introduction", link: "/introduction" },
          { text: "Runs", link: "/runs/" },
          { text: "Benchmarks", link: "/benchmarks/" },
          { text: "Start Benchmark", link: "/start" },
          { text: "Submit Target Request", link: "/submit-target" },
          { text: "Targets", link: "/targets" },
          { text: "Methodology", link: "/methodology" },
        ],
      },
    ],

    outline: { level: [2, 3] },

    search: {
      provider: "local",
      options: {
        detailedView: true,
      },
    },

    socialLinks: [{ icon: "github", link: "https://github.com/Recon-Fuzz/scfuzzbench" }],

    footer: {
      message: "Fully static. Generated in CI from S3 run artifacts.",
      copyright: "Copyright © Recon-Fuzz.",
    },
  },
});
