#!/usr/bin/env node
/**
 * Apollo Daily Lead Gen
 * Targets: Construction | Manufacturing | Semiconductor
 * Roles: Controller, CEO, CFO, President, Owner, AP, AR, Accounting
 * Filter: Companies using Office 365
 */

const fs = require('fs');
const path = require('path');

const APOLLO_API_KEY = process.env.APOLLO_API_KEY;
if (!APOLLO_API_KEY) { console.error('ERROR: APOLLO_API_KEY env var required'); process.exit(1); }
const TODAY = new Date().toISOString().split('T')[0];
const LEADS_DIR = path.join(__dirname, 'leads');
const DEDUP_FILE = path.join(__dirname, 'seen-emails.json');

const NICHES = [
  { name: 'Construction',    keyword: 'construction' },
  { name: 'Manufacturing',   keyword: 'manufacturing' },
  { name: 'Semiconductor',   keyword: 'semiconductor' },
];

const TARGET_TITLES = [
  'Controller',
  'Accounting Manager',
  'Chief Accounting Officer',
  'CEO',
  'Chief Executive Officer',
  'CFO',
  'Chief Financial Officer',
  'President',
  'Owner',
  'Co-Owner',
  'Accounts Payable Manager',
  'Accounts Payable Specialist',
  'Accounts Receivable Manager',
  'Accounts Receivable Specialist',
  'VP Finance',
  'VP of Finance',
  'Director of Finance',
  'Finance Director',
  'Comptroller',
];

const CSV_HEADER = [
  'First Name', 'Last Name', 'Email', 'Title', 'Seniority',
  'Company', 'Industry', 'Company Size', 'Website',
  'Company Phone', 'Direct Phone',
  'City', 'State', 'Country',
  'LinkedIn', 'Niche', 'Date Found'
].join(',');

// ─── Apollo API ────────────────────────────────────────────────────────────

async function apolloSearch(niche, page = 1) {
  const body = {
    api_key: APOLLO_API_KEY,
    page,
    per_page: 100,
    person_titles: TARGET_TITLES,
    q_keywords: niche.keyword,
    contact_email_status: ['verified', 'likely to engage'],
    prospected_by_current_team: ['no'],
    technology_names: ['Office 365', 'Microsoft Office 365', 'Microsoft 365'],
  };

  const res = await fetch('https://api.apollo.io/api/v1/mixed_people/search', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-cache',
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Apollo API ${res.status}: ${text}`);
  }

  return res.json();
}

// ─── Dedup ─────────────────────────────────────────────────────────────────

function loadSeenEmails() {
  const seen = new Set();

  // Load from dedup file
  if (fs.existsSync(DEDUP_FILE)) {
    try {
      const list = JSON.parse(fs.readFileSync(DEDUP_FILE, 'utf-8'));
      list.forEach(e => seen.add(e.toLowerCase()));
      return seen;
    } catch {}
  }

  // Fallback: scan all existing CSVs
  if (fs.existsSync(LEADS_DIR)) {
    const files = fs.readdirSync(LEADS_DIR).filter(f => f.endsWith('.csv'));
    for (const file of files) {
      const lines = fs.readFileSync(path.join(LEADS_DIR, file), 'utf-8').split('\n').slice(1);
      for (const line of lines) {
        const cols = line.split(',');
        const email = cols[2]?.replace(/"/g, '').trim().toLowerCase();
        if (email && email.includes('@')) seen.add(email);
      }
    }
  }

  return seen;
}

function saveSeenEmails(seen) {
  fs.writeFileSync(DEDUP_FILE, JSON.stringify([...seen], null, 2));
}

// ─── CSV ───────────────────────────────────────────────────────────────────

function esc(v) {
  return `"${(v ?? '').toString().replace(/"/g, '""')}"`;
}

function toCSVRow(p, niche) {
  return [
    esc(p.first_name),
    esc(p.last_name),
    esc(p.email),
    esc(p.title),
    esc(p.seniority),
    esc(p.organization?.name),
    esc(p.organization?.industry),
    esc(p.organization?.estimated_num_employees),
    esc(p.organization?.website_url),
    esc(p.organization?.phone),
    esc(p.phone_numbers?.[0]?.sanitized_number),
    esc(p.city),
    esc(p.state),
    esc(p.country),
    esc(p.linkedin_url),
    esc(niche),
    esc(TODAY),
  ].join(',');
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n================================================');
  console.log(`  APOLLO LEAD GEN — ${TODAY}`);
  console.log('================================================');
  console.log('  Niches  : Construction | Manufacturing | Semiconductor');
  console.log('  Roles   : Controller, CEO, CFO, President, Owner, AP, AR...');
  console.log('  Filter  : Office 365 companies only');
  console.log('================================================\n');

  if (!fs.existsSync(LEADS_DIR)) fs.mkdirSync(LEADS_DIR, { recursive: true });

  const seenEmails = loadSeenEmails();
  const startCount = seenEmails.size;
  console.log(`Dedup: ${startCount} previously seen emails loaded\n`);

  const allNewLeads = [];
  const nicheSummary = {};

  for (const niche of NICHES) {
    console.log(`\n[${niche.name}] Searching Apollo...`);
    let page = 1;
    let nicheLeads = 0;
    let totalFetched = 0;

    while (page <= 5) {
      let data;
      try {
        data = await apolloSearch(niche, page);
      } catch (err) {
        console.error(`  ERROR page ${page}: ${err.message}`);
        break;
      }

      const people = data.people || [];
      totalFetched += people.length;

      for (const person of people) {
        const email = person.email?.toLowerCase();
        if (!email || !email.includes('@')) continue;
        if (seenEmails.has(email)) continue;
        seenEmails.add(email);
        allNewLeads.push({ ...person, _niche: niche.name });
        nicheLeads++;
      }

      console.log(`  Page ${page}: ${people.length} fetched, ${nicheLeads} new unique so far`);

      if (people.length < 100) break;
      page++;

      // Respect Apollo rate limits
      await new Promise(r => setTimeout(r, 1200));
    }

    nicheSummary[niche.name] = nicheLeads;
    console.log(`  [${niche.name}] Done — ${nicheLeads} new leads`);
  }

  // Write CSV
  const csvPath = path.join(LEADS_DIR, `${TODAY}.csv`);
  const csvRows = [CSV_HEADER, ...allNewLeads.map(p => toCSVRow(p, p._niche))];
  fs.writeFileSync(csvPath, csvRows.join('\n'), 'utf-8');

  // Save dedup state
  saveSeenEmails(seenEmails);

  const newTotal = seenEmails.size - startCount;

  console.log('\n================================================');
  console.log('  DAILY REPORT');
  console.log('================================================');
  console.log(`  Date          : ${TODAY}`);
  console.log(`  New leads     : ${newTotal}`);
  console.log(`  Pipeline total: ${seenEmails.size}`);
  console.log(`  Output file   : leads/${TODAY}.csv`);
  console.log('  Breakdown:');
  for (const [n, c] of Object.entries(nicheSummary)) {
    console.log(`    ${n.padEnd(18)}: ${c} leads`);
  }
  console.log('================================================\n');

  if (newTotal === 0) {
    console.log('No new leads today (all results were duplicates or no results returned).');
  }

  return newTotal;
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
