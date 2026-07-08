"""Assemble the sweep figures into a single self-contained HTML report.

Embeds each PNG as a base64 data URI (the artifact CSP blocks external hosts),
fills them into the hand-authored template below, and writes report.html.
"""

import base64

RESULTS = "benchmarks/results"


def data_uri(path: str) -> str:
    with open(path, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    return f"data:image/png;base64,{b64}"


HTML = """<title>shapebench — per-worker lock & Wool vs Ray</title>
<style>
  :root {
    --ground: #F4F6F8; --surface: #FFFFFF; --ink: #14181D; --muted: #626C77;
    --hairline: #DDE3E8; --accent: #0E6E8C; --signal: #B4531F;
    --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    --mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
  }
  * { box-sizing: border-box; }
  body {
    background: var(--ground); color: var(--ink); font-family: var(--sans);
    line-height: 1.62; margin: 0; -webkit-font-smoothing: antialiased;
  }
  .wrap { max-width: 940px; margin: 0 auto; padding: 64px 24px 96px; }
  .eyebrow {
    font-family: var(--mono); font-size: 12px; letter-spacing: 0.18em;
    text-transform: uppercase; color: var(--accent); margin: 0 0 14px;
  }
  h1 {
    font-size: clamp(28px, 4.4vw, 42px); line-height: 1.08; font-weight: 650;
    letter-spacing: -0.02em; text-wrap: balance; margin: 0 0 18px; max-width: 20ch;
  }
  .lede { font-size: 18px; color: var(--muted); max-width: 62ch; margin: 0 0 22px; }
  .meta {
    display: flex; flex-wrap: wrap; gap: 8px 10px; font-family: var(--mono);
    font-size: 12.5px; color: var(--muted); padding-top: 18px;
    border-top: 1px solid var(--hairline);
  }
  .meta span { white-space: nowrap; }
  .meta span::after { content: "·"; margin-left: 10px; color: var(--hairline); }
  .meta span:last-child::after { content: ""; }
  section { margin-top: 56px; }
  .sec-label {
    font-family: var(--mono); font-size: 12px; letter-spacing: 0.16em;
    text-transform: uppercase; color: var(--muted);
    display: flex; align-items: center; gap: 12px; margin-bottom: 20px;
  }
  .sec-label::after { content: ""; flex: 1; height: 1px; background: var(--hairline); }
  h2 {
    font-size: 24px; font-weight: 640; letter-spacing: -0.015em;
    margin: 0 0 10px; text-wrap: balance;
  }
  p { margin: 0 0 16px; max-width: 68ch; }
  .fig {
    background: var(--surface); border: 1px solid var(--hairline);
    border-radius: 10px; padding: 18px; margin: 22px 0;
    box-shadow: 0 1px 2px rgba(20,24,29,0.04);
  }
  .fig img { display: block; width: 100%; height: auto; border-radius: 4px; }
  .fig figcaption {
    font-family: var(--mono); font-size: 12.5px; color: var(--muted);
    margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--hairline);
    line-height: 1.5;
  }
  .find {
    border-left: 3px solid var(--signal); background: rgba(180,83,31,0.05);
    padding: 12px 16px; margin: 16px 0; border-radius: 0 6px 6px 0;
    font-size: 15.5px;
  }
  .find b { color: var(--signal); }
  table {
    width: 100%; border-collapse: collapse; font-family: var(--mono);
    font-size: 14px; font-variant-numeric: tabular-nums; margin: 8px 0 4px;
  }
  caption {
    text-align: left; font-family: var(--mono); font-size: 12px;
    letter-spacing: 0.05em; color: var(--muted); margin-bottom: 10px;
  }
  th, td { text-align: right; padding: 9px 14px; border-bottom: 1px solid var(--hairline); }
  th:first-child, td:first-child { text-align: left; }
  thead th { color: var(--muted); font-weight: 500; border-bottom: 1.5px solid var(--ink); }
  tbody tr:last-child td { border-bottom: none; }
  .row-hl td { color: var(--accent); }
  ol.findings { counter-reset: f; list-style: none; padding: 0; margin: 8px 0 0; }
  ol.findings li {
    position: relative; padding: 14px 0 14px 44px;
    border-bottom: 1px solid var(--hairline); max-width: 72ch;
  }
  ol.findings li:last-child { border-bottom: none; }
  ol.findings li::before {
    counter-increment: f; content: counter(f, decimal-leading-zero);
    position: absolute; left: 0; top: 14px; font-family: var(--mono);
    font-size: 12px; color: var(--accent); letter-spacing: 0.05em;
  }
  .k { font-family: var(--mono); font-weight: 600; }
  footer {
    margin-top: 64px; padding-top: 20px; border-top: 1px solid var(--hairline);
    font-family: var(--mono); font-size: 12px; color: var(--muted);
  }
</style>

<div class="wrap">
  <p class="eyebrow">shapebench · dispatch benchmark</p>
  <h1>The per-worker lock, and where Wool stands against Ray</h1>
  <p class="lede">Sweeping every knob in the suite &mdash; shape, structural size, and worker count &mdash; at zero task granularity, so the makespan is pure dispatch plumbing. The question isn&rsquo;t a single speedup number; it&rsquo;s the <em>functional form</em>: how the speedup scales, and where it stops.</p>
  <div class="meta">
    <span>g = 0 (pure plumbing)</span><span>workers 1&ndash;8</span>
    <span>Wool 0.10.0 + PWL prototype</span><span>Ray 2.56</span>
    <span>single node</span><span>p50, warm</span>
  </div>

  <section>
    <p class="sec-label">01 &nbsp; The per-worker lock</p>
    <h2>Where the fix lands, and where it doesn&rsquo;t</h2>
    <p>Speedup of the per-worker lock over the current global lock (base makespan &divide; PWL makespan), across every shape and structural size at each worker count. Fan-out shapes light up; shapes with no concurrent fan-out stay inert &mdash; the signature of a throughput fix, not a latency one.</p>
    <figure class="fig">
      <img alt="Heatmap of per-worker-lock speedup across shape/size and worker count" src="%%HEATMAP%%">
      <figcaption>Speedup by (shape &times; size) down, worker count across. Green = the lock helps; red &asymp; no change. S2 fan-out and S3 scatter-gather deepen with workers and with fan-out width; pipeline, diamond and streaming stay flat.</figcaption>
    </figure>
    <div class="find"><b>Saturating, not linear.</b> The speedup follows Amdahl&rsquo;s law with a serial fraction of ~0.25 &mdash; a hard ceiling near <span class="k">4&times;</span>, ~80% of it reached by W=4. The un-parallelizable part is the single caller issuing every dispatch serially from one event loop (~330&micro;s/dispatch); once the handshakes overlap across workers, more workers do nothing.</div>
    <figure class="fig">
      <img alt="S2 fan-out speedup versus workers and makespan versus fan-out width" src="%%SCALING%%">
      <figcaption>Left: S2 speedup vs workers bends away from the ideal &prop;W line early. Right: makespan vs fan-out width is a straight line in log&ndash;log for both variants (exponent &asymp; 1) &mdash; linear in dispatch count, no quadratic term hiding in the path.</figcaption>
    </figure>
  </section>

  <section>
    <p class="sec-label">02 &nbsp; Wool vs Ray</p>
    <h2>The caller ceiling is universal</h2>
    <p>Normalizing each framework to its own single-worker makespan shows scaling efficiency independent of raw speed. Ray&rsquo;s centralized driver fans out from one process too &mdash; so the question is whether its cheap C++ submission escapes the wall that bounds Wool.</p>
    <figure class="fig">
      <img alt="Self-speedup versus workers for Wool base, Wool with per-worker lock, and Ray" src="%%SCALING_COMPARE%%">
      <figcaption>Self-speedup = makespan(W=1) / makespan(W). Wool base (red) is flat &mdash; the global lock means workers don&rsquo;t help at all. Wool +PWL (orange) and Ray (blue) track each other and saturate at the same ~2&ndash;3&times;, far below ideal &prop;W.</figcaption>
    </figure>
    <div class="find"><b>Ray hits the same ceiling.</b> Single-driver fan-out is caller-bound for both frameworks &mdash; the Amdahl wall isn&rsquo;t a Wool artifact. The per-worker lock doesn&rsquo;t make Wool <em>faster than</em> Ray; it makes Wool <em>scale like</em> Ray. Breaking the ceiling needs caller parallelism (nested dispatch) &mdash; which Wool&rsquo;s async model does and Ray deadlocks on, so S5 is absent from Ray here.</div>
    <table>
      <caption>Fan-out self-speedup ceiling (Amdahl fit, n=128)</caption>
      <thead><tr><th>series</th><th>S2 fan-out</th><th>S3 scatter-gather</th><th>scales with W?</th></tr></thead>
      <tbody>
        <tr><td>Wool base</td><td>1.0&times;</td><td>1.0&times;</td><td>no</td></tr>
        <tr class="row-hl"><td>Wool +PWL</td><td>2.7&times;</td><td>2.9&times;</td><td>yes, ~3&times; cap</td></tr>
        <tr><td>Ray 2.56</td><td>2.0&times;</td><td>2.9&times;</td><td>yes, ~3&times; cap</td></tr>
      </tbody>
    </table>
    <figure class="fig">
      <img alt="Heatmap of Wool-with-PWL to Ray makespan ratio across shapes" src="%%GAP%%">
      <figcaption>Absolute standing after the fix: Wool(+PWL) &divide; Ray makespan. ~2&ndash;3&times; on most shapes (a flat constant-factor gap &mdash; Python path vs C++ core), except streaming, which runs 4&ndash;9&times; slower and worsens with stream length.</figcaption>
    </figure>
  </section>

  <section>
    <p class="sec-label">03 &nbsp; Breaking the ceiling</p>
    <h2>Nested fan-out clears the wall &mdash; and Ray can&rsquo;t follow</h2>
    <p>The flat ceiling is one driver issuing every dispatch serially. Restructure the same 256-way fan-out as a shallow tree of sub-dispatchers and the driver issues only <span class="k">branch</span> coarse dispatches; a Wool routine runs its inner calls <em>locally</em> on the worker, so the remaining leaves never round-trip. The serial dispatch count &mdash; the Amdahl fraction &mdash; drops from N to branch.</p>
    <figure class="fig">
      <img alt="Makespan and speedup vs workers for flat vs nested fan-out, Wool and Ray" src="%%NESTED%%">
      <figcaption>Left: makespan vs workers, N=256. Wool nested (green) is the fastest line &mdash; below even Ray flat; Ray nested (purple) sits at the top, ~470ms. Right: speedup vs flat@W=1 &mdash; flat plateaus at the ~2&times; Amdahl wall while nested climbs past 20&times;.</figcaption>
    </figure>
    <div class="find"><b>Opposite responses to one idiom.</b> Nesting makes Wool <span class="k">~11&times; faster</span> than its own flat fan-out &mdash; and beats Ray flat outright. The identical restructuring makes Ray <span class="k">~28&times; slower</span>, because it keeps every leaf a distributed task and blocks a worker slot per internal node. Measured at g=0: the win is dispatch-overhead collapse via local execution, not added compute parallelism.</div>
  </section>

  <section>
    <p class="sec-label">04 &nbsp; What the sweep says</p>
    <ol class="findings">
      <li>The per-worker lock lifts Wool fan-out from <span class="k">doesn&rsquo;t-scale</span> (global lock, 1.0&times;) to Ray-equivalent scaling &mdash; a measured <span class="k">2.6&ndash;2.8&times;</span> at W=4.</li>
      <li>That speedup is <span class="k">Amdahl-capped near 4&times;</span>, saturating by W&asymp;4. The wall is the single caller&rsquo;s serial dispatch CPU, not the lock.</li>
      <li>Ray&rsquo;s single-driver fan-out saturates at the <span class="k">same ~2&ndash;3&times;</span>. The caller ceiling is universal to centralized fan-out.</li>
      <li>After the fix the residual gap is a <span class="k">constant ~2&ndash;3&times;</span> (Python vs C++), except <span class="k">streaming at 4&ndash;9&times;</span> &mdash; Wool&rsquo;s clearest weak spot.</li>
      <li>Makespan is <span class="k">linear</span> in fan-out width and chain depth &mdash; no quadratic scaling anywhere in the dispatch path.</li>
      <li>Nesting the fan-out clears the wall: Wool nested is <span class="k">~11&times; faster</span> than flat and beats Ray flat, while the same idiom makes Ray <span class="k">~28&times; slower</span>.</li>
    </ol>
  </section>

  <footer>shapebench &middot; g=0 sweep &middot; Wool 0.10.0 + per-worker-lock prototype (#263) &middot; Ray 2.56 &middot; single node, p50 warm</footer>
</div>
"""


def main() -> None:
    html = (
        HTML.replace("%%HEATMAP%%", data_uri(f"{RESULTS}/heatmap.png"))
        .replace("%%SCALING%%", data_uri(f"{RESULTS}/scaling.png"))
        .replace("%%SCALING_COMPARE%%", data_uri(f"{RESULTS}/scaling_compare.png"))
        .replace("%%GAP%%", data_uri(f"{RESULTS}/gap_heatmap.png"))
        .replace("%%NESTED%%", data_uri(f"{RESULTS}/nested.png"))
    )
    with open(f"{RESULTS}/report.html", "w") as fh:
        fh.write(html)
    print(f"wrote {RESULTS}/report.html ({len(html) / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
