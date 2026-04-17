#!/usr/bin/env node
/**
 * Pull all contacts from Apollo "semi" list
 * - Saves emails to seen-emails.json (dedup)
 * - Saves CSV: leads/semi-list.csv
 */
const fs = require('fs');
const path = require('path');

const KEY = process.env.APOLLO_API_KEY;
if (!KEY) { console.error('APOLLO_API_KEY required'); process.exit(1); }

const LABEL_ID  = '69e2415bf2f72a001194a77e';
const DEDUP_FILE = path.join(__dirname, 'seen-emails.json');
const OUT_CSV    = path.join(__dirname, 'leads', 'semi-list.csv');
const sleep      = ms => new Promise(r => setTimeout(r, ms));

async function apolloPost(endpoint, body) {
  const res = await fetch(`https://api.apollo.io/api/v1/${endpoint}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': KEY, 'Cache-Control': 'no-cache' },
    body: JSON.stringify(body),
  });
  if (res.status === 429) { console.log('Rate limited, waiting 30s...'); await sleep(30000); return apolloPost(endpoint, body); }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function main() {
  // Load existing dedup
  const seenEmails = new Set();
  if (fs.existsSync(DEDUP_FILE)) {
    try { JSON.parse(fs.readFileSync(DEDUP_FILE, 'utf-8')).forEach(e => seenEmails.add(e.toLowerCase())); } catch {}
  }
  console.log(`Existing dedup set: ${seenEmails.size} emails`);

  const rows = ['Email,Full Name'];
  let page = 1, total = 0, newCount = 0;

  while (true) {
    const data = await apolloPost('contacts/search', { page, per_page: 100, label_ids: [LABEL_ID] });
    const contacts = data.contacts || [];
    const pagination = data.pagination || {};

    if (page === 1) console.log(`Total contacts in list: ${pagination.total_entries || '?'}`);
    if (contacts.length === 0) break;

    for (const c of contacts) {
      const email = c.email?.toLowerCase()?.trim();
      if (!email || !email.includes('@')) continue;
      total++;
      const name = `${c.first_name || ''} ${c.last_name || ''}`.trim();
      rows.push(`"${email}","${name.replace(/"/g, '""')}"`);
      if (!seenEmails.has(email)) { seenEmails.add(email); newCount++; }
    }

    if (page % 20 === 0) process.stdout.write(`  Page ${page}/${pagination.total_pages || '?'} — ${total} contacts pulled\r`);
    if (contacts.length < 100) break;
    page++;
    await sleep(200);
  }

  // Save CSV
  fs.mkdirSync(path.dirname(OUT_CSV), { recursive: true });
  fs.writeFileSync(OUT_CSV, rows.join('\n'), 'utf-8');

  // Save updated dedup
  fs.writeFileSync(DEDUP_FILE, JSON.stringify([...seenEmails], null, 2));

  console.log(`\nDone!`);
  console.log(`  Contacts with email : ${total}`);
  console.log(`  New to dedup set    : ${newCount}`);
  console.log(`  Total dedup set     : ${seenEmails.size}`);
  console.log(`  CSV saved           : leads/semi-list.csv`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
