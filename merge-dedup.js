#!/usr/bin/env node
/**
 * Merge seen-emails.json with semi list + all CSVs in leads/
 * Run after any bulk job completes to ensure dedup set is complete.
 */
const fs   = require('fs');
const path = require('path');

const KEY        = process.env.APOLLO_API_KEY;
const DEDUP_FILE = path.join(__dirname, 'seen-emails.json');
const LEADS_DIR  = path.join(__dirname, 'leads');
const SEMI_LABEL = '69e2415bf2f72a001194a77e';
const sleep      = ms => new Promise(r => setTimeout(r, ms));

async function apolloPost(endpoint, body) {
  const res = await fetch(`https://api.apollo.io/api/v1/${endpoint}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': KEY, 'Cache-Control': 'no-cache' },
    body: JSON.stringify(body),
  });
  if (res.status === 429) { await sleep(30000); return apolloPost(endpoint, body); }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function main() {
  const merged = new Set();

  // 1. Load existing seen-emails.json (100k run output)
  if (fs.existsSync(DEDUP_FILE)) {
    try {
      JSON.parse(fs.readFileSync(DEDUP_FILE, 'utf-8')).forEach(e => merged.add(e.toLowerCase()));
    } catch {}
  }
  console.log(`From seen-emails.json: ${merged.size}`);

  // 2. Scan all CSV files in leads/
  if (fs.existsSync(LEADS_DIR)) {
    for (const file of fs.readdirSync(LEADS_DIR).filter(f => f.endsWith('.csv'))) {
      const lines = fs.readFileSync(path.join(LEADS_DIR, file), 'utf-8').split('\n').slice(1);
      for (const line of lines) {
        const email = line.split(',')[0]?.replace(/"/g, '').trim().toLowerCase();
        if (email && email.includes('@')) merged.add(email);
      }
    }
  }
  console.log(`After scanning CSVs: ${merged.size}`);

  // 3. Pull all emails from Apollo "semi" list
  if (KEY) {
    console.log('Pulling semi list from Apollo...');
    let page = 1;
    while (true) {
      const data = await apolloPost('contacts/search', { page, per_page: 100, label_ids: [SEMI_LABEL] });
      const contacts = data.contacts || [];
      if (contacts.length === 0) break;
      for (const c of contacts) {
        const email = c.email?.toLowerCase()?.trim();
        if (email && email.includes('@')) merged.add(email);
      }
      if (page % 20 === 0) process.stdout.write(`  Pulled page ${page}...\r`);
      if (contacts.length < 100) break;
      page++;
      await sleep(150);
    }
    console.log(`After semi list: ${merged.size}`);
  }

  // 4. Save merged dedup
  fs.writeFileSync(DEDUP_FILE, JSON.stringify([...merged], null, 2));
  console.log(`\nDone — seen-emails.json now has ${merged.size} unique emails`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
