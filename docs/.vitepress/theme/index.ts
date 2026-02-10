import type { Theme } from "vitepress";
import DefaultTheme from "vitepress/theme";
import "./custom.css";

import StartBenchmark from "../components/StartBenchmark.vue";

export default {
  extends: DefaultTheme,
  enhanceApp(ctx) {
    DefaultTheme.enhanceApp?.(ctx);
    ctx.app.component("StartBenchmark", StartBenchmark);
  },
} satisfies Theme;
