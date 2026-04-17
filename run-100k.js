#!/usr/bin/env node
/**
 * ONE-TIME 100k LEAD RUN
 * Parallel search + enrich across construction, manufacturing, semiconductor
 * USA only | email + name only | no duplicates
 */

const fs   = require('fs');
const path = require('path');

const KEY        = process.env.APOLLO_API_KEY;
if (!KEY) { console.error('APOLLO_API_KEY required'); process.exit(1); }

const TODAY      = new Date().toISOString().split('T')[0];
const LEADS_DIR  = path.join(__dirname, 'leads');
const DEDUP_FILE = path.join(__dirname, 'seen-emails.json');
const OUT_FILE   = path.join(LEADS_DIR, `${TODAY}-100k.csv`);
const LOG_FILE   = path.join(__dirname, 'run-100k.log');
const TARGET     = 100_000;
const CONCURRENCY = 10;         // parallel enrich calls
const BATCH_PAUSE = 150;        // ms between batches
const SEARCH_PAUSE = 500;       // ms between search pages

const NICHES = ['construction', 'manufacturing', 'semiconductor'];

// People who handle payments and monetary transactions
const TITLES = [
  // Accounts Payable
  'Accounts Payable Manager','Accounts Payable Specialist','Accounts Payable Supervisor',
  'Accounts Payable Director','Accounts Payable Clerk','AP Manager','AP Director','AP Specialist',
  // Accounts Receivable
  'Accounts Receivable Manager','Accounts Receivable Specialist','Accounts Receivable Supervisor',
  'Accounts Receivable Director','AR Manager','AR Director','AR Specialist',
  // Controllers & Accounting
  'Controller','Corporate Controller','Plant Controller','Division Controller',
  'Comptroller','Accounting Manager','Chief Accounting Officer','Accounting Director',
  // CFO & Finance Leadership
  'CFO','Chief Financial Officer','VP Finance','VP of Finance',
  'Vice President of Finance','Director of Finance','Finance Director','Finance Manager',
  // Treasury & Billing
  'Treasurer','Treasury Manager','Billing Manager','Billing Director',
  'Payroll Manager','Payroll Director',
  // C-Suite / Owners (decision makers in smaller companies)
  'CEO','Chief Executive Officer','President','Owner','Co-Owner',
];

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(msg) {
  const ts = new Date().toISOString().slice(11,19);
  const line = `[${ts}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function apolloPost(endpoint, body, retry = 0) {
  let res;
  try {
    res = await fetch(`https://api.apollo.io/api/v1/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Api-Key': KEY, 'Cache-Control': 'no-cache' },
      body: JSON.stringify(body),
    });
  } catch (e) {
    if (retry < 3) { await sleep(2000); return apolloPost(endpoint, body, retry + 1); }
    throw e;
  }
  if (res.status === 429) {
    log('Rate limited — waiting 30s...');
    await sleep(30_000);
    return apolloPost(endpoint, body, retry);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// ─── Search: collect Apollo IDs for a niche ──────────────────────────────────

async function collectIds(niche, maxPages = 500) {
  const ids = new Set();
  log(`[${niche}] Starting ID collection (up to ${maxPages} pages)...`);

  for (let page = 1; page <= maxPages; page++) {
    try {
      const data = await apolloPost('mixed_people/api_search', {
        page, per_page: 100,
        person_titles: TITLES,
        q_keywords: niche,
        prospected_by_current_team: ['no'],
        technology_names: ['Office 365', 'Microsoft Office 365', 'Microsoft 365'],
        person_locations: ['United States'],
        contact_email_status: ['verified'],
      });

      const people = data.people || [];
      if (people.length === 0) { log(`[${niche}] No more results at page ${page}`); break; }

      let added = 0;
      for (const p of people) { if (p.id && !ids.has(p.id)) { ids.add(p.id); added++; } }

      if (page % 10 === 0 || people.length < 100) {
        log(`[${niche}] Page ${page}: +${added} IDs (total ${ids.size})`);
      }

      if (people.length < 100) break;
      await sleep(SEARCH_PAUSE);
    } catch (e) {
      log(`[${niche}] Search error page ${page}: ${e.message} — skipping`);
      await sleep(2000);
    }
  }

  log(`[${niche}] ID collection done: ${ids.size} IDs`);
  return [...ids];
}

// ─── Enrich: batch parallel email reveal ─────────────────────────────────────

async function enrichBatch(ids) {
  return Promise.all(ids.map(id =>
    apolloPost('people/enrich', { id, reveal_personal_emails: true })
      .then(d => d.person || null)
      .catch(() => null)
  ));
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  if (!fs.existsSync(LEADS_DIR)) fs.mkdirSync(LEADS_DIR, { recursive: true });
  fs.writeFileSync(LOG_FILE, '');  // reset log

  // Load dedup
  const seenEmails = new Set();
  if (fs.existsSync(DEDUP_FILE)) {
    try { JSON.parse(fs.readFileSync(DEDUP_FILE, 'utf-8')).forEach(e => seenEmails.add(e)); } catch {}
  }
  log(`Loaded ${seenEmails.size} previously seen emails`);

  // Write CSV header
  fs.writeFileSync(OUT_FILE, 'Email,Full Name\n');

  log(`\nTARGET: ${TARGET.toLocaleString()} new leads`);
  log(`Niches: ${NICHES.join(', ')}`);
  log(`Concurrency: ${CONCURRENCY} parallel enrichments\n`);

  const startTime = Date.now();
  let totalNew = 0;
  let totalEnriched = 0;
  let totalWithEmail = 0;

  // Step 1: Collect IDs for all niches in parallel
  log('PHASE 1: Collecting Apollo IDs from all 3 niches...');
  const nicheIds = await Promise.all(NICHES.map(n => collectIds(n, 500)));
  const allIds = [...new Set(nicheIds.flat())];
  log(`\nTotal unique IDs collected: ${allIds.length.toLocaleString()}`);
  log('Shuffling for even niche distribution...');

  // Interleave IDs from each niche for balanced distribution
  const interleaved = [];
  const maxLen = Math.max(...nicheIds.map(a => a.length));
  for (let i = 0; i < maxLen; i++) {
    for (const arr of nicheIds) { if (arr[i]) interleaved.push(arr[i]); }
  }

  log(`\nPHASE 2: Enriching ${interleaved.length.toLocaleString()} IDs for emails...`);

  const writeStream = fs.createWriteStream(OUT_FILE, { flags: 'a' });

  for (let i = 0; i < interleaved.length; i += CONCURRENCY) {
    if (totalNew >= TARGET) break;

    const batch = interleaved.slice(i, i + CONCURRENCY);
    const results = await enrichBatch(batch);
    totalEnriched += batch.length;

    const csvLines = [];
    for (const person of results) {
      if (!person) continue;
      const email = person.email?.toLowerCase();
      if (!email || !email.includes('@')) continue;
      totalWithEmail++;
      if (seenEmails.has(email)) continue;
      seenEmails.add(email);
      const name = `${person.first_name || ''} ${person.last_name || ''}`.trim();
      csvLines.push(`"${email}","${name.replace(/"/g, '""')}"`);
      totalNew++;
      if (totalNew >= TARGET) break;
    }

    if (csvLines.length) writeStream.write(csvLines.join('\n') + '\n');

    // Progress every 1000
    if (totalNew % 1000 === 0 && totalNew > 0) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = totalNew / elapsed;
      const eta = Math.ceil((TARGET - totalNew) / rate);
      const etaMin = Math.ceil(eta / 60);
      log(`Progress: ${totalNew.toLocaleString()}/${TARGET.toLocaleString()} leads | ${(totalWithEmail/totalEnriched*100).toFixed(1)}% hit rate | ETA ~${etaMin}m`);
    }

    await sleep(BATCH_PAUSE);
  }

  writeStream.end();

  // Save dedup
  fs.writeFileSync(DEDUP_FILE, JSON.stringify([...seenEmails], null, 2));

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const hitRate = totalEnriched > 0 ? (totalWithEmail / totalEnriched * 100).toFixed(1) : 0;

  log('\n================================================');
  log('  100K RUN COMPLETE');
  log('================================================');
  log(`  Total new leads  : ${totalNew.toLocaleString()}`);
  log(`  Candidates       : ${totalEnriched.toLocaleString()}`);
  log(`  Email hit rate   : ${hitRate}%`);
  log(`  Time taken       : ${elapsed} minutes`);
  log(`  Output file      : leads/${TODAY}-100k.csv`);
  log(`  Pipeline total   : ${seenEmails.size.toLocaleString()}`);
  log('================================================');
}

main().catch(err => { log('FATAL: ' + err.message); process.exit(1); });
