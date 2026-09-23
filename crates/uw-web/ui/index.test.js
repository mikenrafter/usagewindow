import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('./index.html', import.meta.url), 'utf8');
const start = html.indexOf('function lerp(');
const end = html.indexOf('function burnDisplayWindow', start);
const context = {};
vm.runInNewContext(`${html.slice(start, end)}; globalThis.thresholdOpacity = thresholdOpacity;`, context);

test('threshold opacity uses each threshold remaining budget independently', () => {
  const opacity = context.thresholdOpacity;

  assert.equal(opacity(20, 90), 1, '90% threshold is fully visible below 20% projected remaining');
  assert.equal(opacity(30, 85), 1, '85% threshold is fully visible below 30% projected remaining');
  assert.equal(opacity(15, 95), 0.9444444444444444);
  assert.equal(opacity(15, 90), 1);
  assert.equal(opacity(15, 85), 1);

  assert.ok(opacity(25, 95) < opacity(25, 90));
  assert.ok(opacity(25, 90) < opacity(25, 85));
});
