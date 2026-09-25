import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('./index.html', import.meta.url), 'utf8');
const start = html.indexOf('function lerp(');
const end = html.indexOf('function burnDisplayWindow', start);
const context = {};
vm.runInNewContext(`${html.slice(start, end)}; globalThis.thresholdOpacity = thresholdOpacity;`, context);

const renderStart = html.indexOf('function hslColor(');
const renderEnd = html.indexOf('function setBanner', renderStart);
const renderContext = {
  escapeHtml: value => value,
  remainingHue: () => 180,
  burnDisplayWindow: () => ({ minutes: 30, label: '30m' }),
  countdown: () => '1h 30m',
  durationBetween: () => '30m',
  windowLabel: () => 'test window',
};
vm.runInNewContext(
  `${html.slice(renderStart, renderEnd)}; globalThis.renderUsageBar = renderUsageBar;`,
  renderContext,
);

test('threshold opacity uses each threshold remaining budget independently', () => {
  const opacity = context.thresholdOpacity;

  assert.equal(opacity(20, 90), 1, '90% threshold is fully visible below 20% projected remaining');
  assert.equal(opacity(30, 85), 1, '85% threshold is fully visible below 30% projected remaining');
  assert.equal(opacity(15, 95), 0.75);
  assert.equal(opacity(15, 90), 1);
  assert.equal(opacity(15, 85), 1);
  assert.equal(opacity(30, 90), 0.75);
  assert.equal(opacity(40, 90), 0.5);
  assert.equal(opacity(50, 90), 0);

  assert.ok(opacity(25, 95) < opacity(25, 90));
  assert.ok(opacity(25, 90) < opacity(25, 85));
});

test('added reset time uses the tempo shade', () => {
  const output = renderContext.renderUsageBar('Test', {
    pct: 80,
    burn_rate_pct_per_hour: 10,
    tempo_pct: 5,
    active_burn_pct: 0,
    keptalive_burn_pct: 0,
    inactive_burn_pct: 0,
    depletes_at: '2026-09-23T12:00:00Z',
    resets_at: '2026-09-23T12:30:00Z',
    window: {},
  });

  assert.match(output, /<span class="added-time"[^>]*>1h 30m<\/span> \+ 30m/);
  assert.match(output, /class="added-time"[^>]*color:\s*hsl\(180\.0, 70%, 48%\)/);
});
