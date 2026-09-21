# memJev landing site design

## Status

Approved visual direction, prepared for production implementation.

## Objective

Build a separate, public, production-grade landing site for **memJev** at
`memjev.getmetacognition.com`. The page must make the product legible in one
scroll, create an immediate premium impression, collect waitlist signups, and
direct technical visitors to the open-source repository.

The brand is always rendered as **memJev** in public prose and interface copy.
Lowercase technical identifiers such as package names, repository names, URLs,
and environment variables retain the casing required by their platform.

## Product position

memJev turns successful agent runs into versioned, evidence-backed procedures
and returns the right procedure to future agents through deterministic
eligibility gates and ranking. It is procedural reuse with bounded composition,
not general learning.

Primary audience:

- teams running agents against recurring operational or engineering work;
- agent-framework authors who need inspectable, replayable procedural memory;
- technical early adopters evaluating the open-source release.

Primary action: join the hosted-product waitlist.

Secondary action: view the open-source repository.

## Experience direction

The approved Canvas direction is the source of truth:

- pure `#ffffff` and `#000000`;
- no symbol or illustrated logo;
- `memJev` functions as a typographic wordmark;
- dense first viewport rather than intentional empty space;
- precise grotesk typography with no decorative italics;
- a live trace is the signature visual;
- structural rules, sequence markers, and system labels communicate state;
- no gradients, shadows, decorative cards, stock imagery, or coloured accents.

The page uses five narrative movements:

1. **Hero:** “Stop solving the same task twice.” Copy occupies the left side and
   the live trace engine occupies the right side.
2. **Record:** a raw agent trace and its branches enter as evidence.
3. **Prove:** failed branches collapse and the passing path becomes dominant.
4. **Connect and recall:** verified steps form graph structure and resolve into
   a reusable procedure.
5. **Waitlist:** the result is handed to the next agent and the visitor is
   invited to request early access.

Each movement is a real populated section. No section relies on a tall empty
spacer or CSS sticky positioning to keep its content visible.

## Scroll animation

The trace is one continuous, scroll-controlled object rather than a set of
unrelated entrance effects.

- Scroll progress draws the active path with SVG stroke offsets.
- Failed branches recede only after the passing path has crossed their fork.
- Verified nodes lock into a regular graph as their evidence becomes valid.
- The final graph compresses into a procedure strip and moves toward the
  receiving-agent label.
- Copy transitions are synchronized with the relevant trace state.
- Scrubbing is reversible: scrolling upward restores the prior trace state.
- Motion values are derived from section-local scroll progress, not elapsed
  time, so the same position always produces the same frame.
- A reduced-motion mode shows the final state of each section with no scrubbed
  transforms.

Production implementation will use the Motion library's scroll primitives with
GPU-safe transforms and SVG path animation. Intersection observers defer work
outside the active section. The page must remain fully legible when JavaScript
is unavailable; animation is progressive enhancement.

## Technical architecture

Create a separate repository named `memjev-site`.

- Next.js App Router with TypeScript.
- Static server-rendered page with narrowly scoped client components for the
  trace animation and waitlist interaction.
- Motion for scroll-bound animation.
- CSS Modules and global design tokens; no component framework.
- A self-hosted open-source grotesk font, loaded through `next/font/local`, to
  avoid runtime font requests and layout shift.
- Vercel Analytics only after deployment; no behavioural advertising trackers.
- Metadata, Open Graph image, canonical URL, sitemap, and robots metadata.
- AGPL-3.0 license and a README documenting local development and deployment.

The page is decomposed into:

- `SiteHeader`: typographic wordmark and the two primary routes.
- `Hero`: stable copy layout plus the initial trace state.
- `TraceStory`: four real sections and the shared scroll-progress model.
- `TraceEngine`: SVG renderer that receives normalized deterministic state.
- `ProofStatement`: concise product boundary and inspectability claim.
- `WaitlistForm`: accessible form with explicit pending, success, duplicate,
  invalid, rate-limited, and unavailable states.
- `Footer`: repository, license, privacy, and product ownership links.

## Waitlist data flow

The browser posts an email address and consent timestamp to a same-origin Route
Handler. The handler:

1. validates and normalizes the address;
2. rejects bot-filled honeypot fields;
3. applies IP-derived rate limiting without persisting raw IP addresses;
4. inserts the address idempotently into a managed PostgreSQL table;
5. returns the same success response for new and duplicate addresses to avoid
   account enumeration.

The production database will be a Vercel Marketplace Postgres provider selected
at deployment. The repository includes the schema and migration. The form does
not depend on the memJev procedural-memory API.

No secret is exposed to the browser. Server-only database credentials remain in
Vercel environment variables. Logs exclude submitted email addresses.

## Accessibility and responsive behaviour

- Semantic headings and landmarks.
- Keyboard-visible focus styles.
- Minimum 44px interactive targets.
- Form status announced through an `aria-live` region.
- SVG includes a concise accessible description and is hidden from assistive
  technology when equivalent copy already communicates the same state.
- Layout remains content-dense at tablet and mobile sizes; the trace moves below
  the hero copy rather than disappearing.
- `prefers-reduced-motion` disables scroll scrubbing and scan animation.
- Black/white contrast meets WCAG AAA for body copy.

## Performance budget

- Lighthouse production targets: Performance ≥ 95; Accessibility, Best
  Practices, and SEO ≥ 100 on the landing route.
- Initial JavaScript target below 120 kB gzip.
- No raster hero asset.
- No layout shift from fonts or animation.
- Scroll work uses transforms and SVG stroke properties; no per-frame layout
  reads after section bounds are measured.

## Verification

- Unit tests for trace-state interpolation and email normalization.
- Component tests for waitlist states and reduced-motion rendering.
- Route tests for validation, idempotency, rate limiting, and redacted logs.
- Playwright coverage at desktop and mobile widths for every section, keyboard
  submission, and no-JavaScript legibility.
- Visual regression snapshots for the hero and all four trace states.
- Production build, typecheck, lint, tests, and Lighthouse run before release.
- Post-deployment smoke test against the Vercel URL before custom-domain setup.

## Deployment

The repository is deployed to Vercel from its default branch. After the preview
deployment passes verification:

1. promote the verified build to production;
2. add `memjev.getmetacognition.com` to the Vercel project;
3. use the exact DNS record Vercel reports for that project;
4. verify TLS, canonical metadata, waitlist insertion, and repository links.

The final credential request is limited to resources that cannot be provisioned
through the connected Vercel account: DNS access for
`getmetacognition.com`, the target public repository URL if it differs from
the code host created during implementation, and any required database-provider
authorization.
