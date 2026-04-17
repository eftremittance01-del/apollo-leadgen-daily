#!/usr/bin/env node
/**
 * Apollo Daily Lead Gen — Search → Enrich Pipeline
 * Targets: Construction | Manufacturing | Semiconductor
 * Roles: Controller, CEO, CFO, President, Owner, AP, AR, Accounting
 * Filter: Office 365 companies
 *
 * HOW IT WORKS:
 *   1. api_search  → returns Apollo IDs (no emails, obfuscated names)
 *   2. people/enrich → reveals full name + email for each ID
 */

const fs = require('fs');
const path = require('path');

const APOLLO_API_KEY = process.env.APOLLO_API_KEY;
if (!APOLLO_API_KEY) { console.error('ERROR: APOLLO_API_KEY env var required'); process.exit(1); }

const TODAY = new Date().toISOString().split('T')[0];
const LEADS_DIR = path.join(__dirname, 'leads');
const DEDUP_FILE = path.join(__dirname, 'seen-emails.json');

// How many search pages per niche (100 results each)
// 5 pages = 500 candidates per niche → ~300 emails at 60% hit rate
// Increase to 10-20 for more volume (watch rate limits)
const MAX_PAGES_PER_NICHE = parseInt(process.env.MAX_PAGES || '5');

const NICHES = [
  { name: 'Construction',  keyword: 'construction' },
  { name: 'Manufacturing', keyword: 'manufacturing' },
  { name: 'Semiconductor', keyword: 'semiconductor' },
];

const TARGET_TITLES = [
  'Controller', 'Accounting Manager', 'Chief Accounting Officer', 'Comptroller',
  'CEO', 'Chief Executive Officer',
  'CFO', 'Chief Financial Officer',
  'President', 'Owner', 'Co-Owner',
  'Accounts Payable Manager', 'Accounts Payable Specialist', 'AP Manager',
  'Accounts Receivable Manager', 'Accounts Receivable Specialist', 'AR Manager',
  'VP Finance', 'VP of Finance', 'Director of Finance', 'Finance Director',
];

const CSV_HEADER = [
  'Email', 'First Name', 'Last Name', 'Title', 'Seniority',
  'Company', 'Industry', 'Company Size', 'Website',
  'Company Phone', 'City', 'State', 'Country',
  'LinkedIn', 'Niche', 'Date Found'
].join(',');

// ─── Apollo helpers ───────────────────────────────────────────────────────────

async function apolloPost(endpoint, body) {
  const res = await fetch(`https://api.apollo.io/api/v1/${endpoint}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-cache',
      'X-Api-Key': APOLLO_API_KEY,
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Apollo ${endpoint} HTTP ${res.status}`);
  return res.json();
}

async function searchPage(niche, page) {
  const data = await apolloPost('mixed_people/api_search', {
    page,
    per_page: 100,
    person_titles: TARGET_TITLES,
    q_keywords: niche.keyword,
    prospected_by_current_team: ['no'],
    technology_names: ['Office 365', 'Microsoft Office 365', 'Microsoft 365'],
  });
  return (data.people || []).map(p => p.id).filter(Boolean);
}

async function enrichId(apolloId) {
  const data = await apolloPost('people/enrich', {
    id: apolloId,
    reveal_personal_emails: true,
  });
  return data.person || null;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── Dedup ────────────────────────────────────────────────────────────────────

function loadSeenEmails() {
  const seen = new Set();
  if (fs.existsSync(DEDUP_FILE)) {
    try {
      JSON.parse(fs.readFileSync(DEDUP_FILE, 'utf-8')).forEach(e => seen.add(e.toLowerCase()));
      return seen;
    } catch {}
  }
  if (fs.existsSync(LEADS_DIR)) {
    fs.readdirSync(LEADS_DIR).filter(f => f.endsWith('.csv')).forEach(file => {
      fs.readFileSync(path.join(LEADS_DIR, file), 'utf-8').split('\n').slice(1).forEach(line => {
        const email = line.split(',')[0]?.replace(/"/g, '').trim().toLowerCase();
        if (email && email.includes('@')) seen.add(email);
      });
    });
  }
  return seen;
}

function saveSeenEmails(seen) {
  fs.writeFileSync(DEDUP_FILE, JSON.stringify([...seen], null, 2));
}

// ─── CSV ──────────────────────────────────────────────────────────────────────

function esc(v) { return `"${(v ?? '').toString().replace(/"/g, '""')}"` }

function toCSVRow(p, niche) {
  const org = p.organization || {};
  return [
    esc(p.email), esc(p.first_name), esc(p.last_name), esc(p.title), esc(p.seniority),
    esc(org.name), esc(org.industry), esc(org.estimated_num_employees), esc(org.website_url),
    esc(org.phone), esc(p.city), esc(p.state), esc(p.country),
    esc(p.linkedin_url), esc(niche), esc(TODAY),
  ].join(',');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n================================================');
  console.log(`  APOLLO LEAD GEN — ${TODAY}`);
  console.log('================================================');
  console.log('  Method  : Search → Enrich (2-step email reveal)');
  console.log('  Niches  : Construction | Manufacturing | Semiconductor');
  console.log('  Roles   : Controller, CEO, CFO, President, Owner, AP, AR...');
  console.log('  Filter  : Office 365 companies');
  console.log(`  Pages   : ${MAX_PAGES_PER_NICHE} per niche (${MAX_PAGES_PER_NICHE * 100 * NICHES.length} candidates total)`);
  console.log('================================================\n');

  if (!fs.existsSync(LEADS_DIR)) fs.mkdirSync(LEADS_DIR, { recursive: true });

  const seenEmails = loadSeenEmails();
  const startCount = seenEmails.size;
  console.log(`Dedup: ${startCount} previously seen emails loaded\n`);

  const allNewLeads = [];
  const nicheSummary = {};
  let totalEnriched = 0, totalWithEmail = 0;

  for (const niche of NICHES) {
    console.log(`\n[${niche.name}] Starting...`);
    let nicheLeads = 0;
    let nicheIds = [];

    // Step 1: collect all IDs from search pages
    for (let page = 1; page <= MAX_PAGES_PER_NICHE; page++) {
      try {
        const ids = await searchPage(niche, page);
        nicheIds = nicheIds.concat(ids);
        process.stdout.write(`  Search page ${page}: ${ids.length} IDs found\r`);
        if (ids.length < 100) break;
        await sleep(600);
      } catch (err) {
        console.error(`  Search error page ${page}: ${err.message}`);
        break;
      }
    }
    console.log(`  Collected ${nicheIds.length} IDs from search`);

    // Step 2: enrich each ID for email
    let enriched = 0;
    for (const id of nicheIds) {
      try {
        const person = await enrichId(id);
        totalEnriched++;
        enriched++;
        if (!person) continue;

        const email = person.email?.toLowerCase();
        if (!email || !email.includes('@')) continue;

        totalWithEmail++;
        if (seenEmails.has(email)) continue;

        seenEmails.add(email);
        allNewLeads.push({ ...person, _niche: niche.name });
        nicheLeads++;

        if (nicheLeads % 10 === 0) {
          process.stdout.write(`  Enriched ${enriched}/${nicheIds.length} — ${nicheLeads} new emails so far\r`);
        }

        await sleep(400); // ~2.5 req/sec to avoid rate limit
      } catch (err) {
        if (err.message.includes('429')) {
          console.log('\n  Rate limited — waiting 30s...');
          await sleep(30000);
        } else {
          console.error(`  Enrich error: ${err.message}`);
        }
      }
    }

    nicheSummary[niche.name] = nicheLeads;
    console.log(`  [${niche.name}] Done — ${nicheLeads} new emails from ${nicheIds.length} candidates`);
  }

  // Write CSV
  const csvPath = path.join(LEADS_DIR, `${TODAY}.csv`);
  const csvRows = [CSV_HEADER, ...allNewLeads.map(p => toCSVRow(p, p._niche))];
  fs.writeFileSync(csvPath, csvRows.join('\n'), 'utf-8');
  saveSeenEmails(seenEmails);

  const newTotal = seenEmails.size - startCount;
  const hitRate = totalEnriched > 0 ? ((totalWithEmail / totalEnriched) * 100).toFixed(1) : 0;

  console.log('\n================================================');
  console.log('  DAILY REPORT');
  console.log('================================================');
  console.log(`  Date           : ${TODAY}`);
  console.log(`  Candidates     : ${totalEnriched} enriched`);
  console.log(`  Email hit rate : ${hitRate}%`);
  console.log(`  New leads      : ${newTotal}`);
  console.log(`  Pipeline total : ${seenEmails.size}`);
  console.log(`  Output file    : leads/${TODAY}.csv`);
  console.log('  Breakdown:');
  for (const [n, c] of Object.entries(nicheSummary)) {
    console.log(`    ${n.padEnd(18)}: ${c} emails`);
  }
  console.log('================================================\n');

  // Print 3 sample leads per niche
  if (allNewLeads.length > 0) {
    console.log('  SAMPLE LEADS (up to 3 per niche)');
    console.log('------------------------------------------------');
    for (const niche of NICHES) {
      const samples = allNewLeads.filter(p => p._niche === niche.name).slice(0, 3);
      if (!samples.length) { console.log(`  [${niche.name}] — no new leads`); continue; }
      console.log(`\n  [${niche.name}]`);
      for (const p of samples) {
        const org = p.organization || {};
        console.log(`    Name   : ${p.first_name} ${p.last_name}`);
        console.log(`    Email  : ${p.email}`);
        console.log(`    Title  : ${p.title}`);
        console.log(`    Company: ${org.name}`);
        console.log(`    Website: ${org.website_url || 'N/A'}`);
        console.log('    ---');
      }
    }
    console.log('================================================\n');
  }

  return newTotal;
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
