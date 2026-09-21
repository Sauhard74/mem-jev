# memJev Landing Site Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the approved memJev landing page as a separate public repository, with deterministic premium scroll animation, a production waitlist, and a verified Vercel deployment.

**Architecture:** A Next.js 16 App Router project renders the complete narrative as server components, with two narrow client islands: a Motion-powered SVG trace and the waitlist form. Pure trace interpolation and waitlist-domain functions remain framework-independent and fully unit tested; a same-origin Route Handler persists normalized signups to Neon Postgres.

**Tech Stack:** Node.js 20.9+, pnpm, Next.js 16, React 19, TypeScript, Motion for React, CSS Modules, Vitest, Testing Library, Playwright, Neon serverless Postgres, Vercel.

**Spec:** `docs/superpowers/specs/2026-09-21-memjev-landing-site-design.md`

## Global Constraints

- Create a separate repository at `/Users/sauhardgupta/Documents/ChatGPT/memjev-site`; do not place website code in the procedural-memory service repository.
- Render the public brand exactly as `memJev`; lowercase technical identifiers may remain `memjev`.
- Use only `#ffffff` and `#000000`; opacity is permitted, additional colour values are not.
- Do not add a symbol logo, gradients, shadows, stock imagery, decorative cards, or coloured accents.
- Every long-scroll section must contain real visible content; do not use sticky children inside artificial spacer sections.
- Scroll progress must deterministically produce the same visual frame and must reverse when the visitor scrolls upward.
- JavaScript enhances motion but is not required to read or submit the page.
- Respect `prefers-reduced-motion`, keyboard navigation, semantic landmarks, and 44px minimum interactive targets.
- Do not expose or log emails, database URLs, IP addresses, or secret material.
- License the public repository under AGPL-3.0.

## Review Focus

- A 320px-wide viewport must show the complete headline and trace without horizontal overflow; Task 5 adds the Playwright assertion.
- Reduced-motion visitors must see each final trace state with no continuous scan or scroll scrubbing; Task 4 adds the component test.
- Duplicate email submissions must return the same public success response as first-time submissions; Task 6 adds the route test.
- Concurrent requests from one client must not bypass the rate limit; Task 6 adds an atomic repository test.
- Database unavailability must preserve the submitted address in the form, announce retry guidance, and never claim success; Task 6 adds the form and route tests.

---

## File Structure

```text
memjev-site/
├── .github/workflows/ci.yml              # pull-request and main-branch gates
├── app/
│   ├── api/waitlist/route.ts             # same-origin public waitlist endpoint
│   ├── icon.svg                          # typographic mJ favicon, black/white only
│   ├── globals.css                       # reset, tokens, typography, shared motion rules
│   ├── layout.tsx                        # metadata, font, document shell
│   ├── manifest.ts                       # web manifest
│   ├── opengraph-image.tsx               # generated black/white social image
│   ├── page.module.css                   # landing composition and responsive rules
│   ├── page.tsx                          # server-rendered page assembly
│   ├── robots.ts                         # crawler policy
│   └── sitemap.ts                        # canonical URL
├── components/
│   ├── hero.tsx                          # first viewport and initial trace
│   ├── proof-statement.tsx               # inspectability statement
│   ├── site-footer.tsx                   # repository, license, privacy links
│   ├── site-header.tsx                   # wordmark and primary navigation
│   ├── trace-engine.module.css            # SVG and reduced-motion styles
│   ├── trace-engine.tsx                  # Motion client island
│   ├── trace-story.tsx                   # four populated narrative sections
│   ├── waitlist-form.module.css           # form and status presentation
│   └── waitlist-form.tsx                 # accessible client form
├── lib/
│   ├── trace-model.test.ts               # interpolation and boundary tests
│   ├── trace-model.ts                    # pure normalized trace state
│   ├── waitlist-domain.test.ts           # normalization and validation tests
│   ├── waitlist-domain.ts                # domain types and pure functions
│   ├── waitlist-repository.test.ts       # idempotency and rate-limit tests
│   └── waitlist-repository.ts            # Neon persistence boundary
├── migrations/001_waitlist.sql           # idempotent production schema
├── public/fonts/                         # self-hosted open-source font files
├── tests/
│   ├── landing.spec.ts                   # responsive, no-JS, and scroll flow
│   └── waitlist-route.test.ts            # Route Handler contract
├── .env.example                          # safe variable names only
├── .gitignore                            # dependencies, builds, local secrets
├── AGENTS.md                             # repository-local implementation guidance
├── LICENSE                               # AGPL-3.0 full text
├── README.md                             # local setup, architecture, deployment
├── eslint.config.mjs
├── next.config.ts
├── package.json
├── playwright.config.ts
├── pnpm-lock.yaml
├── tsconfig.json
└── vitest.config.ts
```

### Task 1: Scaffold the separate production repository

**Files:**
- Create: the root configuration files listed above
- Create: `app/layout.tsx`
- Create: `app/page.tsx`
- Test: `tests/brand-contract.test.ts`

**Interfaces:**
- Produces: a buildable Next.js 16 application and the `pnpm check` quality gate used by every later task.

- [ ] **Step 1: Create the empty repository and initial package manifest**

Use `pnpm create next-app@latest memjev-site --ts --eslint --app --src-dir=false --use-pnpm --no-tailwind --import-alias "@/*"` from `/Users/sauhardgupta/Documents/ChatGPT`. Confirm Node is at least 20.9 and pin the generated dependency versions in `pnpm-lock.yaml`.

- [ ] **Step 2: Install runtime and test dependencies**

```bash
pnpm add motion @neondatabase/serverless
pnpm add -D vitest @vitejs/plugin-react jsdom @testing-library/react @testing-library/jest-dom @playwright/test glob
```

Add scripts:

```json
{
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "eslint .",
    "typecheck": "tsc --noEmit",
    "test": "vitest run",
    "test:e2e": "playwright test",
    "check": "pnpm lint && pnpm typecheck && pnpm test && pnpm build"
  }
}
```

- [ ] **Step 3: Write the failing brand contract test**

```ts
import { readFileSync } from "node:fs";
import { globSync } from "glob";
import { describe, expect, it } from "vitest";

describe("public brand contract", () => {
  it("uses memJev and only the approved colours", () => {
    const source = globSync(["app/**/*.{ts,tsx,css}", "components/**/*.{ts,tsx,css}"])
      .map((file) => readFileSync(file, "utf8"))
      .join("\n");
    expect(source).toContain("memJev");
    expect(source).not.toMatch(/MemJev|MEMJEV/);
    expect(new Set(source.match(/#[0-9a-fA-F]{6}/g) ?? [])).toEqual(
      new Set(["#000000", "#ffffff"]),
    );
  });
});
```

- [ ] **Step 4: Run the test and confirm the generated starter fails**

Run: `pnpm vitest run tests/brand-contract.test.ts`
Expected: FAIL because the starter does not contain `memJev`.

- [ ] **Step 5: Replace starter copy and configuration with the minimal shell**

Create a semantic layout with metadata title `memJev — deterministic memory for agents`, canonical base `https://memjev.getmetacognition.com`, and a placeholder main containing the correctly cased wordmark. Configure Vitest with jsdom and Testing Library setup.

- [ ] **Step 6: Run the initial quality gate**

Run: `pnpm lint && pnpm typecheck && pnpm test && pnpm build`
Expected: all commands exit 0.

- [ ] **Step 7: Initialize git and commit**

```bash
git init -b main
git add -A
git commit -m "chore: scaffold memJev landing site"
```

### Task 2: Establish the design system and static page structure

**Files:**
- Create: `public/fonts/GeistVariable.woff2`
- Modify: `app/layout.tsx`
- Modify: `app/globals.css`
- Modify: `app/page.tsx`
- Create: `app/page.module.css`
- Create: `components/site-header.tsx`
- Create: `components/hero.tsx`
- Create: `components/proof-statement.tsx`
- Create: `components/site-footer.tsx`
- Test: `tests/brand-contract.test.ts`

**Interfaces:**
- Produces: `SiteHeader`, `Hero`, `ProofStatement`, and `SiteFooter` server components.
- Consumes: the build and test configuration from Task 1.

- [ ] **Step 1: Extend the failing contract test**

Assert that the rendered page contains one `h1`, a main landmark, navigation labelled `Primary`, links to `#system` and `#waitlist`, and the headline “Stop solving the same task twice.”

- [ ] **Step 2: Verify the semantic test fails**

Run: `pnpm vitest run tests/brand-contract.test.ts`
Expected: FAIL on the missing sections and headline.

- [ ] **Step 3: Implement tokens and typography**

Define only these colour tokens:

```css
:root {
  --ink: #000000;
  --paper: #ffffff;
  --page-gutter: clamp(1.375rem, 4vw, 4rem);
  --rule: 1px solid currentColor;
}
```

Load the local variable font with `next/font/local`, set `adjustFontFallback: "Arial"`, and expose it as `--font-sans`. Add focus-visible, selection, and reduced-motion base rules.

- [ ] **Step 4: Implement the static composition**

Build the dense 72px-header / content / 58px-footer hero grid, proof statement, and footer from the approved Canvas. Keep lines under 80 characters, provide descriptive link names, and keep every section visible without JavaScript.

- [ ] **Step 5: Verify desktop and mobile CSS mechanically**

Run: `pnpm vitest run tests/brand-contract.test.ts && pnpm lint && pnpm typecheck`
Expected: all commands exit 0.

- [ ] **Step 6: Commit**

```bash
git add app components public/fonts tests
git commit -m "feat: build memJev landing structure"
```

### Task 3: Build the deterministic trace-state model

**Files:**
- Create: `lib/trace-model.ts`
- Create: `lib/trace-model.test.ts`

**Interfaces:**
- Produces: `getTraceState(progress: number): TraceState`.
- Produces: `TraceState = { progress; activeStep; pathLength; branchOpacity; graphOrder; procedureOffset }`.
- Consumes: normalized progress from Motion in Task 4.

- [ ] **Step 1: Write boundary and interpolation tests**

```ts
expect(getTraceState(-1)).toEqual(getTraceState(0));
expect(getTraceState(2)).toEqual(getTraceState(1));
expect(getTraceState(0).activeStep).toBe(0);
expect(getTraceState(0.51).activeStep).toBe(2);
expect(getTraceState(1).pathLength).toBe(1);
expect(getTraceState(1).branchOpacity).toBe(0);
expect(getTraceState(0.75).graphOrder).toBeGreaterThan(getTraceState(0.5).graphOrder);
```

- [ ] **Step 2: Verify tests fail**

Run: `pnpm vitest run lib/trace-model.test.ts`
Expected: FAIL because `getTraceState` does not exist.

- [ ] **Step 3: Implement clamped piecewise interpolation**

Keep the function pure. Use a small exported `lerp(from, to, amount)` helper and explicit ranges for record `[0,.25]`, prove `[.25,.5]`, connect `[.5,.75]`, and recall `[.75,1]`. Do not read the DOM or time.

- [ ] **Step 4: Verify model tests and typecheck**

Run: `pnpm vitest run lib/trace-model.test.ts && pnpm typecheck`
Expected: all commands exit 0.

- [ ] **Step 5: Commit**

```bash
git add lib/trace-model.ts lib/trace-model.test.ts
git commit -m "feat: model deterministic trace states"
```

### Task 4: Implement the premium trace engine

**Files:**
- Create: `components/trace-engine.tsx`
- Create: `components/trace-engine.module.css`
- Create: `components/trace-engine.test.tsx`

**Interfaces:**
- Produces: `TraceEngine({ progress, reducedMotion, label }: TraceEngineProps)`.
- Consumes: `getTraceState` from Task 3 and Motion values from Task 5.

- [ ] **Step 1: Write the failing component tests**

Render the SVG at progress `0`, `.5`, and `1`; assert the accessible label, active-path dash offset, failed-branch opacity, verified-node count, and final procedure-strip state. Render with `reducedMotion=true` and assert that the final state is shown without the animated scan element.

- [ ] **Step 2: Verify tests fail**

Run: `pnpm vitest run components/trace-engine.test.tsx`
Expected: FAIL because `TraceEngine` does not exist.

- [ ] **Step 3: Implement one stable SVG scene**

Use a single fixed `viewBox="0 0 760 610"`. Animate only `transform`, `opacity`, and SVG path stroke properties. Keep node coordinates constant and derive every presentation value from `TraceState`. The visual must include the raw field, failed branches, successful path, graph nodes, and final procedure strip.

- [ ] **Step 4: Add the orchestrated scan**

Use one scan line as the only time-based ambient animation. Stop it when the trace is offscreen or reduced motion is active. Do not add per-node entrance animations.

- [ ] **Step 5: Verify**

Run: `pnpm vitest run components/trace-engine.test.tsx lib/trace-model.test.ts && pnpm typecheck`
Expected: all commands exit 0.

- [ ] **Step 6: Commit**

```bash
git add components/trace-engine* lib
git commit -m "feat: animate the memJev trace engine"
```

### Task 5: Connect motion to four populated scroll sections

**Files:**
- Create: `components/trace-story.tsx`
- Modify: `app/page.tsx`
- Modify: `app/page.module.css`
- Create: `tests/landing.spec.ts`

**Interfaces:**
- Produces: `TraceStory`, the only component that maps section-local scroll position to trace progress.
- Consumes: `TraceEngine` and the four approved copy records.

- [ ] **Step 1: Write failing Playwright scenarios**

Assert that:

- all four headings are visible after scrolling their real sections into view;
- the page contains no blank viewport-sized interval;
- scrolling down advances the `data-trace-progress` value and scrolling up lowers it;
- `document.documentElement.scrollWidth === document.documentElement.clientWidth` at 320px;
- JavaScript-disabled navigation still exposes every headline and waitlist form.

- [ ] **Step 2: Verify the scenarios fail**

Run: `pnpm build && pnpm playwright test tests/landing.spec.ts`
Expected: FAIL because `TraceStory` and progress attributes are missing.

- [ ] **Step 3: Implement four real sections**

Map the four sequence records to four `min-height: 100svh` articles. Each article contains its own copy and a trace visual; do not create a `400vh` wrapper or use `position: sticky`.

- [ ] **Step 4: Add deterministic Motion scroll binding**

Use `useScroll({ target, offset: ["start end", "end start"] })` from `motion/react` on each real section. Transform its progress into that section's quarter of the global trace. Publish the normalized value as a rounded `data-trace-progress` attribute for regression tests.

- [ ] **Step 5: Implement responsive and reduced-motion modes**

At tablet widths, stack copy above the SVG. At 320px, use fluid type and prevent SVG overflow. Under reduced motion, pass the section's final static progress and render no time-based scan.

- [ ] **Step 6: Verify**

Run: `pnpm test && pnpm build && pnpm playwright test tests/landing.spec.ts`
Expected: all commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add app components tests
git commit -m "feat: tell the memJev story through scroll"
```

### Task 6: Implement the production waitlist

**Files:**
- Create: `lib/waitlist-domain.ts`
- Create: `lib/waitlist-domain.test.ts`
- Create: `lib/waitlist-repository.ts`
- Create: `lib/waitlist-repository.test.ts`
- Create: `migrations/001_waitlist.sql`
- Create: `app/api/waitlist/route.ts`
- Create: `components/waitlist-form.tsx`
- Create: `components/waitlist-form.module.css`
- Create: `tests/waitlist-route.test.ts`
- Create: `.env.example`

**Interfaces:**
- Produces: `normalizeEmail(value: unknown): string | null`.
- Produces: `submitWaitlist(request: WaitlistRequest): Promise<WaitlistResult>`.
- Produces: `POST /api/waitlist` with public outcomes `accepted | invalid | rate_limited | unavailable`.
- Consumes: `DATABASE_URL`, `WAITLIST_HASH_SECRET`, and `WAITLIST_ENCRYPTION_KEY` on the server only.

- [ ] **Step 1: Write failing domain tests**

Cover whitespace, Unicode domain normalization, empty input, malformed addresses, addresses over 254 characters, and mixed-case domains. Pin the normalized result without logging the input.

- [ ] **Step 2: Verify domain tests fail**

Run: `pnpm vitest run lib/waitlist-domain.test.ts`
Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the pure domain functions**

Return `null` for invalid input. Preserve the local part as entered after trimming, lowercase the domain, and produce a separate HMAC-SHA-256 lookup digest using `WAITLIST_HASH_SECRET`.

- [ ] **Step 4: Write failing repository and route tests**

Test first insert, duplicate insert, atomic limit increment, concurrent limit rejection, unavailable database, honeypot acceptance without persistence, and identical public success bodies for new and duplicate addresses. Assert no response or captured log contains the email.

- [ ] **Step 5: Verify repository and route tests fail**

Run: `pnpm vitest run lib/waitlist-repository.test.ts tests/waitlist-route.test.ts`
Expected: FAIL because persistence and the route do not exist.

- [ ] **Step 6: Implement schema and repository**

`migrations/001_waitlist.sql` creates:

```sql
create table if not exists waitlist_signups (
  email_digest text primary key,
  encrypted_email text not null,
  consented_at timestamptz not null,
  created_at timestamptz not null default now()
);
create table if not exists waitlist_rate_limits (
  client_digest text not null,
  window_started_at timestamptz not null,
  request_count integer not null,
  primary key (client_digest, window_started_at)
);
```

Use one Postgres transaction for limit enforcement and signup insertion. Encrypt the stored address with AES-256-GCM using `WAITLIST_ENCRYPTION_KEY`; store nonce, authentication tag, and ciphertext together. Derive a rotating client digest from the address and current UTC date with `WAITLIST_HASH_SECRET`; never store a raw IP address.

- [ ] **Step 7: Implement Route Handler and form**

The form uses `method="post" action="/api/waitlist"` so it works without JavaScript. The Route Handler accepts form data and redirects to `/?waitlist=accepted#waitlist`; the client enhancement intercepts submission and requests JSON. It keeps the email on failure, disables only while pending, and announces every state through `aria-live="polite"`. The success copy is identical for first and duplicate submissions.

- [ ] **Step 8: Verify waitlist behaviour**

Run: `pnpm vitest run lib/waitlist-domain.test.ts lib/waitlist-repository.test.ts tests/waitlist-route.test.ts && pnpm typecheck`
Expected: all commands exit 0.

- [ ] **Step 9: Commit**

```bash
git add app/api components/waitlist* lib/waitlist* migrations .env.example tests/waitlist-route.test.ts
git commit -m "feat: add secure memJev waitlist"
```

### Task 7: Add metadata, policy, CI, and release verification

**Files:**
- Create: `app/manifest.ts`
- Create: `app/robots.ts`
- Create: `app/sitemap.ts`
- Create: `app/opengraph-image.tsx`
- Create: `app/icon.svg`
- Create: `.github/workflows/ci.yml`
- Create: `LICENSE`
- Create: `README.md`
- Create: `AGENTS.md`
- Create: `playwright.config.ts`
- Modify: `next.config.ts`

**Interfaces:**
- Produces: complete public metadata, an AGPL-3.0 repository, and mandatory CI gates.

- [ ] **Step 1: Add failing metadata assertions**

Extend `tests/landing.spec.ts` to assert canonical URL, title, description, Open Graph image, icon, sitemap, robots output, repository link, AGPL link, and a restrictive security-header baseline.

- [ ] **Step 2: Verify metadata tests fail**

Run: `pnpm build && pnpm playwright test tests/landing.spec.ts`
Expected: FAIL on missing metadata and headers.

- [ ] **Step 3: Implement metadata and headers**

Generate the Open Graph image with the exact `memJev` casing and black/white palette. Configure CSP, `Referrer-Policy: strict-origin-when-cross-origin`, `X-Content-Type-Options: nosniff`, and `Permissions-Policy` denying unused capabilities.

- [ ] **Step 4: Add CI and documentation**

CI runs `pnpm install --frozen-lockfile`, `pnpm check`, installs Playwright Chromium, and runs `pnpm test:e2e`. README includes local setup, environment variables, migration, architecture, accessibility, and deployment. Copy the complete AGPL-3.0 license.

- [ ] **Step 5: Run the complete local release gate**

Run: `pnpm check && pnpm exec playwright install chromium && pnpm test:e2e`
Expected: all commands exit 0.

- [ ] **Step 6: Run visual and performance review**

Capture desktop `1440x1000`, tablet `834x1112`, and mobile `390x844` screenshots for the hero and all four states. Inspect each image for empty viewports, clipping, casing, and line collisions. Run Lighthouse against `pnpm start`; meet Performance ≥95 and Accessibility/Best Practices/SEO =100 or document and fix the exact failing audit before continuing.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "chore: prepare memJev site for release"
```

### Task 8: Publish the repository and deploy to Vercel

**Files:**
- Modify only if deployment verification reveals a reproducible defect.

**Interfaces:**
- Consumes: the complete green repository from Tasks 1–7.
- Produces: public source repository, Vercel production URL, connected Neon database, and verified waitlist.

- [ ] **Step 1: Recheck secrets and repository state**

Run:

```bash
git status --short
git ls-files | rg '(^|/)(\.env|.*key.*|.*secret.*)$' && exit 1 || true
pnpm check
```

Expected: clean git state, no tracked secret files, and a passing quality gate.

- [ ] **Step 2: Create the public GitHub repository**

Confirm `gh auth status`, then run:

```bash
gh repo create memjev-site --public --source=. --remote=origin --push
```

Do not use a Codex-hosted repository because it cannot satisfy the approved public-source requirement.

- [ ] **Step 3: Create and link the Vercel project**

Use the connected Vercel deployment tools to inspect teams, choose the team connected to the GitHub repository, create `memjev-site`, and trigger the default-branch deployment. Do not invent a deployment URL.

- [ ] **Step 4: Provision Neon through Vercel Marketplace**

Provision a Neon Postgres resource for the Vercel project so credentials are injected automatically. Add independently generated 32-byte `WAITLIST_HASH_SECRET` and `WAITLIST_ENCRYPTION_KEY` values as server-only production and preview variables. Apply `migrations/001_waitlist.sql` using the provider's authenticated migration path.

- [ ] **Step 5: Verify the deployment before adding the domain**

Against the returned `vercel.app` URL:

- load every section at desktop and mobile widths;
- submit a controlled waitlist address once and again;
- verify identical success responses and one stored row;
- confirm security headers, metadata, reduced motion, and repository links;
- verify no client bundle or response exposes either server variable.

- [ ] **Step 6: Attach the custom subdomain**

Add `memjev.getmetacognition.com` to the verified Vercel project. Inspect the project-specific DNS requirement and report the exact CNAME value; do not use a generic A record. After the user applies DNS, verify Vercel ownership status, TLS, the canonical URL, and the production waitlist.

- [ ] **Step 7: Record final release evidence**

Add the live URL and build instructions to README, commit, push, wait for the final deployment, and report the GitHub URL, deployment URL, production status, database status, custom-domain status, and the exact remaining user action if DNS is pending.

## Plan self-review

- Spec coverage: page narrative, deterministic motion, real populated sections, waitlist, accessibility, performance, metadata, licensing, separate repository, Vercel, Neon, and custom domain are each assigned to a task.
- Placeholder scan: every step names concrete files, commands, outcomes, and failure handling.
- Type consistency: `TraceState`, `getTraceState`, `TraceEngineProps`, `normalizeEmail`, `submitWaitlist`, and public Route Handler outcomes are defined before their consumers.
- Review focus: mobile overflow, reduced motion, duplicate privacy, concurrent rate limiting, and database failure each have an explicit test in the owning task.
