const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Pool, types: pgTypes } = require('pg');
const path = require('path');

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'change-this-secret';
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin123';
const ADMIN_NAME = process.env.ADMIN_NAME || 'مدير الحسابات';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && /render\.com|neon\.tech/.test(process.env.DATABASE_URL)
    ? { rejectUnauthorized: false }
    : false
});
// An idle connection dropped by the database (e.g. a Neon compute that went to sleep) must not crash the server
pool.on('error', (err) => console.error('Idle database client error:', err.message));

// ---------------------------------------------------------------------------
// One-time data copy from another database (used to move to a new database host).
// Runs only when MIGRATE_FROM_DATABASE_URL is set AND the target database is empty.
// The whole copy is a single transaction; if anything does not match, nothing is kept.
// ---------------------------------------------------------------------------
const MIGRATION_TABLES = [
  ['partners', null], ['users', null], ['companies', null], ['company_partners', 'percentage'],
  ['projects', null], ['project_partners', 'percentage'], ['entries', 'amount'],
  ['expense_payments', 'amount'], ['current_account', 'amount'], ['inventory_items', 'unit_price'],
  ['inventory_sales', 'sale_amount'], ['sale_collections', 'amount'], ['company_expenses', 'amount'],
  ['expense_items', null]
];

async function migrateFromSourceIfRequested() {
  const srcUrl = process.env.MIGRATE_FROM_DATABASE_URL;
  if (!srcUrl) return;

  let existing = 0;
  for (const [t] of MIGRATION_TABLES) {
    existing += (await pool.query(`SELECT COUNT(*)::int AS c FROM ${t}`)).rows[0].c;
  }
  if (existing > 0) {
    console.log('MIGRATION skipped: the target database already contains data.');
    return;
  }

  // Read dates/timestamps from the source as raw text so nothing shifts between time zones
  const src = new Pool({
    connectionString: srcUrl,
    ssl: /render\.com|neon\.tech/.test(srcUrl) ? { rejectUnauthorized: false } : false,
    types: { getTypeParser: (oid, fmt) => ([1082, 1114, 1184].includes(oid) ? (v => v) : pgTypes.getTypeParser(oid, fmt)) }
  });
  src.on('error', (err) => console.error('Migration source client error:', err.message));

  const client = await pool.connect();
  try {
    console.log('MIGRATION started: copying data from the source database...');
    await client.query('BEGIN');
    const report = [];
    for (const [table, sumCol] of MIGRATION_TABLES) {
      const srcRows = (await src.query(`SELECT * FROM ${table} ORDER BY id`)).rows;
      const srcSum = sumCol
        ? (await src.query(`SELECT COALESCE(SUM(${sumCol}),0)::text AS s FROM ${table}`)).rows[0].s
        : null;
      if (srcRows.length) {
        const cols = Object.keys(srcRows[0]).map(c => `"${c}"`).join(', ');
        for (let i = 0; i < srcRows.length; i += 1000) {
          await client.query(
            `INSERT INTO ${table} (${cols}) SELECT ${cols} FROM json_populate_recordset(NULL::${table}, $1::json)`,
            [JSON.stringify(srcRows.slice(i, i + 1000))]
          );
        }
        await client.query(
          `SELECT setval(pg_get_serial_sequence('${table}', 'id'), (SELECT MAX(id) FROM ${table}), true)`
        );
      }
      const dstCount = (await client.query(`SELECT COUNT(*)::int AS c FROM ${table}`)).rows[0].c;
      const dstSum = sumCol
        ? (await client.query(`SELECT COALESCE(SUM(${sumCol}),0)::text AS s FROM ${table}`)).rows[0].s
        : null;
      if (dstCount !== srcRows.length || (sumCol && Number(dstSum) !== Number(srcSum))) {
        throw new Error(`Mismatch in ${table}: source ${srcRows.length} rows / ${srcSum}, target ${dstCount} rows / ${dstSum}`);
      }
      report.push(`${table}=${dstCount}`);
    }
    await client.query('COMMIT');
    console.log('MIGRATION OK -> ' + report.join(', '));
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('MIGRATION FAILED (nothing was copied):', e.message);
    throw e;
  } finally {
    client.release();
    await src.end().catch(() => {});
  }
}

const app = express();
app.use(cors());
app.use(express.json({ limit: '15mb' }));

// Default expense items (chart of items). Seeded once when the expense_items table is empty.
const DEFAULT_EXPENSE_CHART = [{"n":"تكاليف العمليات","c":[{"n":"تكاليف ارض المشروع","c":[{"n":"ارض المشروع - قيمه العقد"},{"n":"ارض المشروع - اوفر شراء"},{"n":"ارض المشروع - رسوم انهاء إجراءات"},{"n":"اتعاب اشراف"},{"n":"ارض المشروع -رسوم تمويل عقاري"},{"n":"اتعاب اشراف -رسومات وتصميمات هندسية"}]},{"n":"تكلفه اعمال اعتياديه","c":[{"n":"حفر - رمل - زلط"},{"n":"اعمال الاساسات و الخرسانه"},{"n":"اعمال المبانى و الطوب"},{"n":"اعمال البياض"},{"n":"اعمال الدهانات"},{"n":"اعمال العزل"},{"n":"اعمال الكهرباء"},{"n":"اعمال الرخام"},{"n":"اعمال السيراميك و البلاط"},{"n":"اعمال نجاره باب و شباك"},{"n":"اعمال نجاره مسلحه"},{"n":"حديد مسلح"},{"n":"اعمال صحى و سباكه"},{"n":"اعمال الومنيوم"},{"n":"اعمال حديد المشغول - كريتال"},{"n":"اعمال الزجاج"},{"n":"اعمال الهدم و التشويينات"},{"n":"اعمال جبس و فيوتيك"},{"n":"اعمال ديكورات"},{"n":"اعمال التكييفات"},{"n":"اعمال المصاعد و السلالم"},{"n":"اعمال الارصفه و الحدائق"},{"n":"اعمال تجهيزات الموقع"},{"n":"اعمال متنوعه و مستجده"},{"n":"اعمال الزراعات"}]},{"n":"تكاليف غير مباشره","c":[{"n":"مرتبات مواقع"},{"n":"مصاريف انتقالات مشاريع"},{"n":"عده و نقل عده"},{"n":"مصاريف ضيافه مواقع"},{"n":"مصاريف نثريه و اكراميات"},{"n":"مياه و كهرباء و غاز"},{"n":"مصاريف متنوعه"},{"n":"ايجار معدات"},{"n":"امن و حراسه"},{"n":"عمولات و سمسره مشاريع"},{"n":"فروقات استرجاع وحدات"},{"n":"ارباح مساهمين في المشروع"},{"n":"عوائد استثمار المقدم عملاء رواسين"},{"n":"اقفال تكاليف مشروعات"},{"n":"حساب تخفيض تكلفة جاردينيا 1"}]}]},{"n":"مصاريف عموميه و اداريه","c":[{"n":"المرتبات و مزايا عينيه","c":[{"n":"مرتبات الموظفين و الاداره","c":[{"n":"مرتبات اعضاء مجلس الاداره"},{"n":"مرتبات الموظفيين"},{"n":"خصومات وجزءات الموظفين"}]},{"n":"مكافات و حوافز و بدلات"},{"n":"مزايا عينيه","c":[{"n":"مزايا عينيه - رحلات"},{"n":"مزايا عينيه - منح و دراسات تعليميه"},{"n":"مزايا عينيه - مصاريف تاميين علاجى"},{"n":"مزايا عينيه - منح مناسبات"},{"n":"مزايا عينية-يونيفرم"}]},{"n":"عمولات تعيينات"}]},{"n":"مصاريف انتقالات و سفر و اقامه"},{"n":"مال الله - صدقات"},{"n":"مصاريف نثريه"},{"n":"مصاريف بنكيه"},{"n":"فوائد مدينه"},{"n":"مصروف اهلاك أصول ثابته"},{"n":"رسوم حكوميه و اشتراكات"},{"n":"مصروفات تأسيس"},{"n":"مطبوعات اداريه"},{"n":"مصروفات ضيافه و نظافه"},{"n":"هدايا و اكراميات"},{"n":"مصاريف مياه و كهرباء و غاز"},{"n":"مصاريف تليفونات و نت و كروت شحن"},{"n":"مصارييف ادوات مكتبيه"},{"n":"مصاريف الصيانه"},{"n":"تامينات اجتماعية"},{"n":"ايجارات"},{"n":"مصاريف نقل وشحن"},{"n":"مصاريف ماليه و قانونيه","c":[{"n":"اتعاب ماليه"},{"n":"اتعاب مكتب بيت المحاسبه"},{"n":"اتعاب قانونيه"},{"n":"اتعاب محاماه"}]},{"n":"مصاريف بنزين سيارات"},{"n":"مصاريف اجتماعات اداريه و تدريب اداريه"},{"n":"مصاريف تجهيز المقر الجديد"},{"n":"فروق تبديل عملات"},{"n":"مصاريف اداره التطوير و التخطيط"},{"n":"مصاريف اداره الموارد البشريه"},{"n":"مصاريف اشتراكات برامج(اودو-سماك)"},{"n":"خصم مسموح به"},{"n":"اقفال مصروفات عموميه و اداريه"}]},{"n":"مصروفات بيعيه و تسويقيه","c":[{"n":"عمولات مبيعات"},{"n":"حملات دعائيه تفاعليه & انفلونسرز"},{"n":"حملات سوشيال ميديا - مموله"},{"n":"باحث ( SEO )"},{"n":"مطبوعات دعايه"},{"n":"هدايا دعائيه"},{"n":"مؤتمرات و ندوات استثمار عقارى"},{"n":"وكاله حفلات و ايفنتات"},{"n":"برنامج ( CRM )"},{"n":"استبيان عملاء و منافسين"},{"n":"مصروفات تدريب و تطوير تسويقى"},{"n":"تصميمات تسويقيه"},{"n":"اقفال مصاريف بيعيه و ترويجيه"},{"n":"اعمال براندينج و تشطيبات داخليه"},{"n":"حملات دعايه خارجيه- يفط و اعلانات خارجيه"},{"n":"اعمال علاقات عامه PR"},{"n":"حملات دعائيه تصوير و انتاج فيديوهات"},{"n":"لينكد ان"},{"n":"خدمات ميلات جوجل"},{"n":"مصاريف نقل وشحن قسم التسويق"},{"n":"مصروفات انفستوميتر","c":[{"n":"مصروفات عوائد انفستوميتر - استثمارى","c":[{"n":"مصروفات عوائد انفستوميتر استثمار - طار ق العراقى_6"},{"n":"مصروفات عوائد انفستوميتر استثمار - الفت نان_1"},{"n":"مصروفات عوائد انفستوميتر استثمار - طار ق عيد _1"},{"n":"مصروفات عوائد انفستوميتر استثمار - محمد المراغي_1"},{"n":"مصروفات عوائد انفستوميتر استثمار - نائل جمال_1"},{"n":"مصروفات عوائد افستوميتر استثمار -شيماء شمس"},{"n":"مصروفات عوائد انفستوميتر استثمار-اشرف صلاح محمود-1"},{"n":"مصروفات عوائد انفستوميتر استثمار - مني احمد حسن"},{"n":"مصروفات عوائد انفستوميتر استثمار - محمود ناصر"},{"n":"مصروفات عوائد انفستوميتر استثمار-محمد طارق العراقي"},{"n":"مصروفات عوائد انفستوميتر استثمار - محمد علام"},{"n":"مصروفات عوائد انفستوميتر استثمار - علاء الدين صابر"},{"n":"مصروفات عوائد انفستوميتر استثمار - محمد فرج محمد"},{"n":"مصروفات عوائد انفستوميتر استثمار - محمود محمد محمد"},{"n":"مصروفات عوائد انفستوميتر استثمار - هناء حمدان"},{"n":"مصروفات عوائد انفستوميتر استثمار - ماريو مجدي"},{"n":"مصروفات عوائد انفستوميتر استثمار - البي السيد"},{"n":"مصروفات عوائد انفستوميتر استثمار - ياسر شكري"},{"n":"مصروفات عوائد انفستوميتر استثمار - عادل السيد عبد"},{"n":"مصروفات عوائد استثمار -عرابي محمد عبد الحميد"},{"n":"مصروفات عوائد انفستوميتر - تملك"}]},{"n":"عملاء انفستوميتر - على عبد الواحد_1"},{"n":"عملاء انفستوميتر - سعد عبد العال_2"},{"n":"عملاء انفستوميتر - عمرو السيد_1"},{"n":"عملاء انفستوميتر - اندرو منير_1"},{"n":"عملاء انفستوميتر - اشرف كمال_2"},{"n":"عملاء انفستوميتر - عبده عبيد_1"},{"n":"عملاء انفستوميتر - ايمن عبيد _1"},{"n":"مصروفات عمولات التسويق - انفستوميتر"},{"n":"اقفال مصروفات انفستوميتر"}]}]}];

// Same normalisation the frontend uses to match an item name with a category text
function normCat(s) {
  return String(s == null ? '' : s)
    .replace(/[\u064B-\u065F\u0670\u0640]/g, '')
    .replace(/[أإآ]/g, 'ا').replace(/ى/g, 'ي').replace(/ة/g, 'ه')
    .replace(/\s*-\s*/g, '-').replace(/\s+/g, ' ').trim();
}

function isValidISODate(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d) && d.toISOString().slice(0, 10) === s;
}

// ---------------------------------------------------------------------------
// One-time migration: the current account (partner deposits/withdrawals/
// distributions and opening balances) moves from being per-project to being
// per-company. Runs once, guarded by a row in schema_migrations, and is
// itself a single transaction so it either fully applies or not at all.
// ---------------------------------------------------------------------------
async function migrateCurrentAccountToCompanyLevel() {
  await pool.query(`CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
  const already = await pool.query(`SELECT 1 FROM schema_migrations WHERE name='current_account_to_company_v1'`);
  if (already.rows.length) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1) Point every existing current_account row at its project's company (where known)
    await client.query(`
      UPDATE current_account ca SET company_id = p.company_id
      FROM projects p WHERE p.id = ca.project_id AND ca.company_id IS NULL
    `);

    // 2) Merge each project's opening_balance into its company's opening_balance
    //    (summed across every project of that company the partner had a balance in).
    //    A company_partners row is created automatically if the partner wasn't added
    //    to that company yet, with percentage=0 so no money is silently dropped.
    const toMerge = (await client.query(`
      SELECT p.company_id, pp.partner_id, SUM(pp.opening_balance) AS total
      FROM project_partners pp
      JOIN projects p ON p.id = pp.project_id
      WHERE p.company_id IS NOT NULL AND pp.opening_balance <> 0
      GROUP BY p.company_id, pp.partner_id
    `)).rows;
    for (const row of toMerge) {
      await client.query(`
        INSERT INTO company_partners (company_id, partner_id, percentage, opening_balance)
        VALUES ($1, $2, 0, $3)
        ON CONFLICT (company_id, partner_id)
        DO UPDATE SET opening_balance = company_partners.opening_balance + EXCLUDED.opening_balance
      `, [row.company_id, row.partner_id, row.total]);
    }

    const orphanCount = (await client.query(
      `SELECT COUNT(*)::int AS c FROM current_account WHERE company_id IS NULL`
    )).rows[0].c;

    await client.query(
      `INSERT INTO schema_migrations (name) VALUES ('current_account_to_company_v1')`
    );
    await client.query('COMMIT');
    console.log(`MIGRATION current_account_to_company_v1 applied. Projects merged: ${toMerge.length}. Orphan current_account rows (project had no company): ${orphanCount}.`);
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('MIGRATION current_account_to_company_v1 FAILED (nothing changed):', e.message);
    throw e;
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------------------
// One-time fix: items imported before the "building" column existed. Their unit
// code already starts with the court/building ("C1.G.01" -> C1), so fill the empty
// building from it. Runs once (marker row), only touches empty buildings, never
// touches sales or anything else, so it can't duplicate or delete data.
// ---------------------------------------------------------------------------
async function backfillInventoryBuildingFromCode() {
  await pool.query(`CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
  const done = await pool.query(`SELECT 1 FROM schema_migrations WHERE name='inventory_building_from_code_v1'`);
  if (done.rows.length) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query(`
      UPDATE inventory_items
      SET building = substring(name from '^(C[0-9]+)\\.')
      WHERE (building IS NULL OR TRIM(building) = '') AND name ~ '^C[0-9]+\\.'
    `);
    await client.query(`INSERT INTO schema_migrations (name) VALUES ('inventory_building_from_code_v1')`);
    await client.query('COMMIT');
    console.log(`MIGRATION inventory_building_from_code_v1 applied. Items given a building from their code: ${r.rowCount}.`);
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('MIGRATION inventory_building_from_code_v1 FAILED (nothing changed):', e.message);
    throw e;
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------------------
// Schema bootstrap (idempotent) + admin seed
// ---------------------------------------------------------------------------
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role IN ('admin','partner')),
      name TEXT NOT NULL,
      partner_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS partners (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS companies (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS company_partners (
      id SERIAL PRIMARY KEY,
      company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
      partner_id INTEGER NOT NULL REFERENCES partners(id) ON DELETE CASCADE,
      percentage NUMERIC NOT NULL,
      UNIQUE(company_id, partner_id)
    );
    ALTER TABLE company_partners ADD COLUMN IF NOT EXISTS opening_balance NUMERIC NOT NULL DEFAULT 0;
    CREATE TABLE IF NOT EXISTS expense_items (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      parent_id INTEGER REFERENCES expense_items(id) ON DELETE CASCADE,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS company_expenses (
      id SERIAL PRIMARY KEY,
      company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
      category TEXT,
      amount NUMERIC NOT NULL,
      description TEXT,
      entry_date DATE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS company_assets (
      id SERIAL PRIMARY KEY,
      company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      category TEXT,
      purchase_date DATE NOT NULL,
      cost NUMERIC NOT NULL,
      useful_life_years NUMERIC,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE company_expenses ADD COLUMN IF NOT EXISTS asset_id INTEGER REFERENCES company_assets(id) ON DELETE CASCADE;
    CREATE TABLE IF NOT EXISTS projects (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL
    );
    ALTER TABLE projects ADD COLUMN IF NOT EXISTS company_id INTEGER REFERENCES companies(id);
    CREATE TABLE IF NOT EXISTS project_partners (
      id SERIAL PRIMARY KEY,
      project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      partner_id INTEGER NOT NULL REFERENCES partners(id) ON DELETE CASCADE,
      percentage NUMERIC NOT NULL,
      UNIQUE(project_id, partner_id)
    );
    ALTER TABLE project_partners ADD COLUMN IF NOT EXISTS opening_balance NUMERIC NOT NULL DEFAULT 0;
    CREATE TABLE IF NOT EXISTS entries (
      id SERIAL PRIMARY KEY,
      project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      kind TEXT NOT NULL CHECK (kind IN ('revenue','expense')),
      amount NUMERIC NOT NULL,
      description TEXT,
      entry_date DATE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE entries ADD COLUMN IF NOT EXISTS category TEXT;
    CREATE TABLE IF NOT EXISTS expense_payments (
      id SERIAL PRIMARY KEY,
      entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
      amount NUMERIC NOT NULL,
      payment_date DATE NOT NULL,
      description TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS current_account (
      id SERIAL PRIMARY KEY,
      project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
      partner_id INTEGER NOT NULL REFERENCES partners(id) ON DELETE CASCADE,
      kind TEXT NOT NULL CHECK (kind IN ('deposit','withdrawal','distribution')),
      amount NUMERIC NOT NULL,
      description TEXT,
      entry_date DATE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE current_account ALTER COLUMN project_id DROP NOT NULL;
    ALTER TABLE current_account ADD COLUMN IF NOT EXISTS company_id INTEGER REFERENCES companies(id);
    CREATE TABLE IF NOT EXISTS inventory_items (
      id SERIAL PRIMARY KEY,
      project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      unit TEXT,
      quantity_in NUMERIC NOT NULL DEFAULT 0,
      unit_price NUMERIC NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS status TEXT;
    ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS net_area NUMERIC;
    ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS garden_area NUMERIC;
    ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS building TEXT;
    CREATE TABLE IF NOT EXISTS inventory_sales (
      id SERIAL PRIMARY KEY,
      project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      item_id INTEGER NOT NULL REFERENCES inventory_items(id) ON DELETE CASCADE,
      quantity NUMERIC NOT NULL,
      sale_amount NUMERIC NOT NULL,
      sale_date DATE NOT NULL,
      description TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS customer_name TEXT;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS building_no TEXT;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS garage_value NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS garage_collected NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS maintenance_value NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS maintenance_collected NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS utilities_value NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS utilities_collected NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS bank_collected NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS bank_held NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS collection_diff NUMERIC NOT NULL DEFAULT 0;
    ALTER TABLE inventory_sales ADD COLUMN IF NOT EXISTS down_payment_percent NUMERIC;
    CREATE TABLE IF NOT EXISTS sale_collections (
      id SERIAL PRIMARY KEY,
      sale_id INTEGER NOT NULL REFERENCES inventory_sales(id) ON DELETE CASCADE,
      amount NUMERIC NOT NULL,
      collection_date DATE NOT NULL,
      description TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await migrateFromSourceIfRequested();
  await migrateCurrentAccountToCompanyLevel();
  await backfillInventoryBuildingFromCode();

  // Seed the default expense items once
  const itemCount = (await pool.query('SELECT COUNT(*)::int AS c FROM expense_items')).rows[0].c;
  if (itemCount === 0) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const insertNodes = async (nodes, parentId) => {
        let order = 0;
        for (const n of nodes) {
          const r = await client.query(
            'INSERT INTO expense_items (name, parent_id, sort_order) VALUES ($1,$2,$3) RETURNING id',
            [n.n, parentId, order++]
          );
          if (n.c) await insertNodes(n.c, r.rows[0].id);
        }
      };
      await insertNodes(DEFAULT_EXPENSE_CHART, null);
      await client.query('COMMIT');
      console.log('Seeded default expense items');
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('Failed to seed expense items', e);
    } finally {
      client.release();
    }
  }

  // Make sure these top-level categories exist even on databases seeded before they were added
  {
    const allNames = (await pool.query('SELECT name FROM expense_items')).rows.map(r => normCat(r.name));
    for (const extraName of ['مصاريف استثمارية', 'مصاريف تمويلية', ASSET_PURCHASE_CATEGORY]) {
      if (!allNames.includes(normCat(extraName))) {
        const maxOrder = (await pool.query(
          'SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM expense_items WHERE parent_id IS NULL'
        )).rows[0].n;
        await pool.query('INSERT INTO expense_items (name, parent_id, sort_order) VALUES ($1, NULL, $2)', [extraName, maxOrder]);
        console.log(`Seeded extra top-level expense category: ${extraName}`);
      }
    }
  }

  const { rows } = await pool.query('SELECT COUNT(*)::int AS c FROM users');
  if (rows[0].c === 0) {
    const hash = await bcrypt.hash(ADMIN_PASSWORD, 10);
    await pool.query(
      'INSERT INTO users (username, password_hash, role, name) VALUES ($1,$2,$3,$4)',
      [ADMIN_USERNAME, hash, 'admin', ADMIN_NAME]
    );
    console.log(`Seeded admin user "${ADMIN_USERNAME}". Change the password after first login.`);
  }
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------
function sign(user) {
  return jwt.sign(
    { id: user.id, username: user.username, role: user.role, name: user.name, partnerId: user.partner_id },
    JWT_SECRET,
    { expiresIn: '30d' }
  );
}

function auth(req, res, next) {
  const header = req.headers.authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'يجب تسجيل الدخول' });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    return res.status(401).json({ error: 'الجلسة منتهية، سجّل الدخول مرة أخرى' });
  }
}

function requireAdmin(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'هذا الإجراء متاح للمدير فقط' });
  next();
}

// ---------------------------------------------------------------------------
// Auth routes
// ---------------------------------------------------------------------------
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'أدخل اسم المستخدم وكلمة المرور' });
  const { rows } = await pool.query('SELECT * FROM users WHERE username=$1', [username]);
  const user = rows[0];
  if (!user) return res.status(401).json({ error: 'اسم المستخدم أو كلمة المرور غير صحيحة' });
  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return res.status(401).json({ error: 'اسم المستخدم أو كلمة المرور غير صحيحة' });
  res.json({
    token: sign(user),
    user: { id: user.id, username: user.username, role: user.role, name: user.name, partnerId: user.partner_id }
  });
});

app.put('/api/me/password', auth, async (req, res) => {
  const { oldPassword, newPassword } = req.body || {};
  if (!newPassword || newPassword.length < 4) return res.status(400).json({ error: 'كلمة المرور الجديدة قصيرة جدًا' });
  const { rows } = await pool.query('SELECT * FROM users WHERE id=$1', [req.user.id]);
  const user = rows[0];
  const ok = await bcrypt.compare(oldPassword || '', user.password_hash);
  if (!ok) return res.status(401).json({ error: 'كلمة المرور الحالية غير صحيحة' });
  const hash = await bcrypt.hash(newPassword, 10);
  await pool.query('UPDATE users SET password_hash=$1 WHERE id=$2', [hash, req.user.id]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Admin: partners
// ---------------------------------------------------------------------------
app.get('/api/partners', auth, requireAdmin, async (req, res) => {
  const { rows } = await pool.query(`
    SELECT p.id, p.name, u.username
    FROM partners p LEFT JOIN users u ON u.partner_id = p.id
    ORDER BY p.id
  `);
  res.json(rows);
});

app.post('/api/partners', auth, requireAdmin, async (req, res) => {
  const { name, username, password } = req.body || {};
  if (!name || !username || !password) return res.status(400).json({ error: 'كل الحقول مطلوبة' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const existing = await client.query('SELECT id FROM users WHERE username=$1', [username]);
    if (existing.rows.length) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'اسم المستخدم موجود بالفعل' });
    }
    const partnerRes = await client.query('INSERT INTO partners (name) VALUES ($1) RETURNING id', [name]);
    const partnerId = partnerRes.rows[0].id;
    const hash = await bcrypt.hash(password, 10);
    await client.query(
      'INSERT INTO users (username, password_hash, role, name, partner_id) VALUES ($1,$2,$3,$4,$5)',
      [username, hash, 'partner', name, partnerId]
    );
    await client.query('COMMIT');
    res.json({ id: partnerId, name, username });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'حدث خطأ أثناء إضافة الشريك' });
  } finally {
    client.release();
  }
});

// Admin resets a partner's password
app.put('/api/partners/:id/password', auth, requireAdmin, async (req, res) => {
  const { password } = req.body || {};
  if (!password || password.length < 4) return res.status(400).json({ error: 'كلمة المرور قصيرة جدًا' });
  const hash = await bcrypt.hash(password, 10);
  const result = await pool.query('UPDATE users SET password_hash=$1 WHERE partner_id=$2', [hash, req.params.id]);
  if (result.rowCount === 0) return res.status(404).json({ error: 'لا يوجد حساب دخول لهذا الشريك' });
  res.json({ ok: true });
});

// Admin renames a partner's username
app.put('/api/partners/:id/username', auth, requireAdmin, async (req, res) => {
  const { username } = req.body || {};
  if (!username) return res.status(400).json({ error: 'اسم المستخدم مطلوب' });
  try {
    const result = await pool.query('UPDATE users SET username=$1 WHERE partner_id=$2', [username, req.params.id]);
    if (result.rowCount === 0) return res.status(404).json({ error: 'لا يوجد حساب دخول لهذا الشريك' });
    res.json({ ok: true });
  } catch (e) {
    res.status(409).json({ error: 'اسم المستخدم مستخدم بالفعل' });
  }
});

app.delete('/api/partners/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM users WHERE partner_id=$1', [req.params.id]);
  await pool.query('DELETE FROM partners WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Admin: companies (each project belongs to a company; partners have a
// percentage share at the company level, separate from their per-project share)
// ---------------------------------------------------------------------------
app.get('/api/companies', auth, requireAdmin, async (req, res) => {
  const companies = (await pool.query('SELECT id, name FROM companies ORDER BY id')).rows;
  const shares = (await pool.query(`
    SELECT cp.company_id, cp.partner_id, cp.percentage, cp.opening_balance, p.name AS partner_name
    FROM company_partners cp JOIN partners p ON p.id = cp.partner_id
  `)).rows;
  const withShares = companies.map(c => ({
    ...c,
    partners: shares.filter(s => s.company_id === c.id)
      .map(s => ({ partnerId: s.partner_id, percentage: Number(s.percentage), openingBalance: Number(s.opening_balance), name: s.partner_name }))
  }));
  res.json(withShares);
});

app.post('/api/companies', auth, requireAdmin, async (req, res) => {
  const { name } = req.body || {};
  if (!name) return res.status(400).json({ error: 'اسم الشركة مطلوب' });
  const { rows } = await pool.query('INSERT INTO companies (name) VALUES ($1) RETURNING *', [name]);
  res.json(rows[0]);
});

app.put('/api/companies/:id', auth, requireAdmin, async (req, res) => {
  const { name } = req.body || {};
  if (!name || !name.trim()) return res.status(400).json({ error: 'اسم الشركة مطلوب' });
  const { rows } = await pool.query('UPDATE companies SET name=$1 WHERE id=$2 RETURNING *', [name.trim(), req.params.id]);
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

app.delete('/api/companies/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('UPDATE projects SET company_id=NULL WHERE company_id=$1', [req.params.id]);
  await pool.query('DELETE FROM companies WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/api/companies/:id/partners', auth, requireAdmin, async (req, res) => {
  const { partnerId, percentage, openingBalance } = req.body || {};
  if (!partnerId || percentage === undefined) return res.status(400).json({ error: 'بيانات ناقصة' });
  try {
    const { rows } = await pool.query(
      'INSERT INTO company_partners (company_id, partner_id, percentage, opening_balance) VALUES ($1,$2,$3,$4) RETURNING *',
      [req.params.id, partnerId, percentage, openingBalance || 0]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(409).json({ error: 'هذا الشريك مضاف بالفعل لهذه الشركة' });
  }
});

app.put('/api/companies/:id/partners/:partnerId', auth, requireAdmin, async (req, res) => {
  const { percentage, openingBalance } = req.body || {};
  const { rows } = await pool.query(
    `UPDATE company_partners SET
       percentage = COALESCE($1, percentage),
       opening_balance = COALESCE($2, opening_balance)
     WHERE company_id=$3 AND partner_id=$4 RETURNING *`,
    [percentage === undefined ? null : percentage, openingBalance === undefined ? null : openingBalance, req.params.id, req.params.partnerId]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

app.delete('/api/companies/:id/partners/:partnerId', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM company_partners WHERE company_id=$1 AND partner_id=$2', [req.params.id, req.params.partnerId]);
  res.json({ ok: true });
});

// Company-level general/administrative expenses (separate from each project's own costs)
app.get('/api/companies/:id/expenses', auth, requireAdmin, async (req, res) => {
  const { rows } = await pool.query(
    'SELECT * FROM company_expenses WHERE company_id=$1 ORDER BY entry_date DESC, created_at DESC',
    [req.params.id]
  );
  res.json(rows);
});

app.post('/api/companies/:id/expenses', auth, requireAdmin, async (req, res) => {
  const { category, amount, date, description } = req.body || {};
  if (!amount || !date) return res.status(400).json({ error: 'بيانات ناقصة' });
  const { rows } = await pool.query(
    'INSERT INTO company_expenses (company_id, category, amount, description, entry_date) VALUES ($1,$2,$3,$4,$5) RETURNING *',
    [req.params.id, category || 'أخرى', amount, description || null, date]
  );
  res.json(rows[0]);
});

app.put('/api/companies/:id/expenses/:expenseId', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const bad = checkMoneyEdit(b);
  if (bad) return res.status(400).json({ error: bad });
  const cur = (await pool.query('SELECT asset_id FROM company_expenses WHERE id=$1 AND company_id=$2', [req.params.expenseId, req.params.id])).rows[0];
  if (!cur) return res.status(404).json({ error: 'غير موجود' });
  if (cur.asset_id) return res.status(409).json({ error: 'ده مصروف شراء أصل ثابت. عدّله من صفحة «الأصول الثابتة» وهيتعدل هنا تلقائي.' });
  const v = (x) => (x === undefined ? null : x);
  const { rows } = await pool.query(
    `UPDATE company_expenses SET amount = COALESCE($1, amount), entry_date = COALESCE($2, entry_date),
       description = COALESCE($3, description), category = COALESCE($4, category)
     WHERE id=$5 AND company_id=$6 RETURNING *`,
    [v(b.amount), v(b.date), v(b.description), v(b.category), req.params.expenseId, req.params.id]
  );
  res.json(rows[0]);
});
// rows tied to an asset purchase keep the asset's purchase date, so they are left out of a bulk date change
registerBulkDate('/api/companies/:id/expenses/bulk-date', 'company_expenses', 'entry_date',
  { sql: 'company_id = $3 AND asset_id IS NULL', params: (req) => [parseInt(req.params.id, 10)] });

app.delete('/api/companies/:id/expenses/:expenseId', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM company_expenses WHERE id=$1 AND company_id=$2', [req.params.expenseId, req.params.id]);
  res.json({ ok: true });
});

app.post('/api/companies/:id/expenses/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  await pool.query('DELETE FROM company_expenses WHERE id = ANY($1::int[]) AND company_id=$2', [ids, req.params.id]);
  res.json({ ok: true, deleted: ids.length });
});

app.post('/api/companies/:id/expenses/bulk', auth, requireAdmin, async (req, res) => {
  const { rows } = req.body || {};
  if (!Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  const valid = rows.filter(r => r.amount && r.date && isValidISODate(r.date));
  if (!valid.length) return res.status(400).json({ error: 'لا يوجد صفوف صالحة للاستيراد (تحقق من صيغة التاريخ)' });
  try {
    await pool.query(
      `INSERT INTO company_expenses (company_id, category, amount, description, entry_date)
       SELECT $1, u.category, u.amount, u.description, u.entry_date
       FROM UNNEST($2::text[], $3::numeric[], $4::text[], $5::date[]) AS u(category, amount, description, entry_date)`,
      [req.params.id, valid.map(r => r.category || 'أخرى'), valid.map(r => r.amount),
       valid.map(r => r.description || null), valid.map(r => r.date)]
    );
    res.json({ inserted: valid.length });
  } catch (e) {
    console.error('company expenses bulk import failed', e);
    res.status(500).json({ error: 'فشل الاستيراد: ' + e.message });
  }
});

// ---------------------------------------------------------------------------
// Fixed assets register (سجل الأصول الثابتة), per company. Straight-line
// depreciation is computed on read from cost, purchase_date and useful_life_years
// (when given) — nothing to store or keep in sync.
// ---------------------------------------------------------------------------
const ASSET_PURCHASE_CATEGORY = 'شراء أصول';

// Registers the purchase of the given assets as company expenses (category "شراء أصول", dated the purchase date).
// Assets that already have their expense are skipped, so it is safe to call again.
async function recordAssetPurchases(db, companyId, assetIds) {
  const { rows } = await db.query(`
    INSERT INTO company_expenses (company_id, category, amount, description, entry_date, asset_id)
    SELECT a.company_id, $2, a.cost, 'شراء أصل: ' || a.name, a.purchase_date, a.id
    FROM company_assets a
    WHERE a.company_id = $1 AND a.id = ANY($3::int[])
      AND NOT EXISTS (SELECT 1 FROM company_expenses e WHERE e.asset_id = a.id)
    RETURNING id
  `, [companyId, ASSET_PURCHASE_CATEGORY, assetIds]);
  return rows.length;
}

// Validates the editable fields of a money record. Returns an error string, or null when fine.
function checkMoneyEdit(b) {
  if (b.amount !== undefined && b.amount !== null && (!Number.isFinite(Number(b.amount)) || Number(b.amount) === 0)) return 'المبلغ غير صحيح';
  if (b.date !== undefined && b.date !== null && !isValidISODate(b.date)) return 'التاريخ غير صالح (الصيغة YYYY-MM-DD)';
  return null;
}

// "Change the date of the selected rows": either one new date for all of them, or move each by N days (+1 / -1 ...).
// Handy for fixing a whole import whose dates came out a day off, without editing rows one by one.
function registerBulkDate(path, table, dateCol, extraWhere) {
  app.post(path, auth, requireAdmin, async (req, res) => {
    const { ids, date, shiftDays } = req.body || {};
    if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
    const idList = ids.map(n => parseInt(n, 10)).filter(Boolean);
    const params = [idList];
    let setSql;
    if (date !== undefined && date !== null && date !== '') {
      if (!isValidISODate(date)) return res.status(400).json({ error: 'التاريخ غير صالح (الصيغة YYYY-MM-DD)' });
      params.push(date); setSql = `${dateCol} = $2::date`;
    } else if (Number.isInteger(Number(shiftDays)) && Number(shiftDays) !== 0 && Math.abs(Number(shiftDays)) <= 3660) {
      params.push(Number(shiftDays)); setSql = `${dateCol} = (${dateCol} + $2::int)`;
    } else {
      return res.status(400).json({ error: 'حدد تاريخًا جديدًا أو عدد أيام للإزاحة' });
    }
    let where = `id = ANY($1::int[])`;
    if (extraWhere) { where += ' AND ' + extraWhere.sql; params.push(...extraWhere.params(req)); }
    const { rowCount } = await pool.query(`UPDATE ${table} SET ${setSql} WHERE ${where}`, params);
    res.json({ updated: rowCount });
  });
}

function withAssetDepreciation(row) {
  const cost = Number(row.cost);
  const life = row.useful_life_years == null ? null : Number(row.useful_life_years);
  let accumulatedDepreciation = 0;
  let bookValue = cost;
  let ageYears = null;
  if (life && life > 0) {
    const purchase = new Date(row.purchase_date);
    ageYears = Math.max(0, (Date.now() - purchase.getTime()) / (365.25 * 86400 * 1000));
    const annual = cost / life;
    accumulatedDepreciation = Math.min(cost, annual * ageYears);
    bookValue = cost - accumulatedDepreciation;
  }
  return { ...row, cost, usefulLifeYears: life, accumulatedDepreciation, bookValue };
}

app.get('/api/companies/:id/assets', auth, requireAdmin, async (req, res) => {
  const { rows } = await pool.query(`
    SELECT a.*, EXISTS (SELECT 1 FROM company_expenses e WHERE e.asset_id = a.id) AS has_expense
    FROM company_assets a WHERE a.company_id = $1 ORDER BY a.purchase_date DESC, a.created_at DESC
  `, [req.params.id]);
  res.json(rows.map(withAssetDepreciation));
});

// recordExpense (default true): also book the purchase as a company expense under "شراء أصول"
app.post('/api/companies/:id/assets', auth, requireAdmin, async (req, res) => {
  const { name, category, purchaseDate, cost, usefulLifeYears, notes, recordExpense } = req.body || {};
  if (!name || !purchaseDate || !cost) return res.status(400).json({ error: 'بيانات ناقصة' });
  if (!isValidISODate(purchaseDate)) return res.status(400).json({ error: 'تاريخ الشراء غير صالح' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      'INSERT INTO company_assets (company_id, name, category, purchase_date, cost, useful_life_years, notes) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *',
      [req.params.id, name, category || null, purchaseDate, cost, usefulLifeYears || null, notes || null]
    );
    let expenses = 0;
    if (recordExpense !== false) expenses = await recordAssetPurchases(client, req.params.id, [rows[0].id]);
    await client.query('COMMIT');
    res.json({ ...withAssetDepreciation(rows[0]), has_expense: expenses > 0 });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('add asset failed', e);
    res.status(500).json({ error: 'تعذر إضافة الأصل: ' + e.message });
  } finally { client.release(); }
});

app.put('/api/companies/:id/assets/:assetId', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const v = (x) => (x === undefined ? null : x);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      `UPDATE company_assets SET
         name = COALESCE($1, name),
         category = COALESCE($2, category),
         purchase_date = COALESCE($3, purchase_date),
         cost = COALESCE($4, cost),
         useful_life_years = COALESCE($5, useful_life_years),
         notes = COALESCE($6, notes)
       WHERE id=$7 AND company_id=$8 RETURNING *`,
      [v(b.name), v(b.category), v(b.purchaseDate), v(b.cost), v(b.usefulLifeYears), v(b.notes), req.params.assetId, req.params.id]
    );
    if (!rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'غير موجود' }); }
    // keep the linked purchase expense in step with the asset
    await client.query(
      `UPDATE company_expenses SET amount=$1, entry_date=$2, description='شراء أصل: ' || $3 WHERE asset_id=$4`,
      [rows[0].cost, rows[0].purchase_date, rows[0].name, rows[0].id]
    );
    await client.query('COMMIT');
    res.json(withAssetDepreciation(rows[0]));
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'تعذر تعديل الأصل: ' + e.message });
  } finally { client.release(); }
});

// Deleting an asset also deletes the "شراء أصول" expense that was booked for it (FK cascade).
app.delete('/api/companies/:id/assets/:assetId', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM company_assets WHERE id=$1 AND company_id=$2', [req.params.assetId, req.params.id]);
  res.json({ ok: true });
});

app.post('/api/companies/:id/assets/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  const del = await pool.query('DELETE FROM company_assets WHERE id = ANY($1::int[]) AND company_id=$2 RETURNING id', [ids, req.params.id]);
  res.json({ ok: true, deleted: del.rowCount });
});

// For assets that were registered without a purchase expense (older ones, or "don't book it" at the time)
app.post('/api/companies/:id/assets/record-expenses', auth, requireAdmin, async (req, res) => {
  const ids = Array.isArray((req.body || {}).ids) ? req.body.ids.map(n => parseInt(n, 10)).filter(Boolean) : [];
  if (!ids.length) return res.status(400).json({ error: 'لا يوجد أصول محددة' });
  const created = await recordAssetPurchases(pool, req.params.id, ids);
  res.json({ created, alreadyHadExpense: ids.length - created });
});

// Change the purchase date of many assets at once (one new date, or move each by N days). The "شراء أصول" expense booked
// for each asset gets the same date, so the register and the expenses never disagree.
app.post('/api/companies/:id/assets/bulk-date', auth, requireAdmin, async (req, res) => {
  const { ids, date, shiftDays } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد أصول محددة' });
  const idList = ids.map(n => parseInt(n, 10)).filter(Boolean);
  const cid = parseInt(req.params.id, 10);
  const params = [idList, cid];
  let setSql;
  if (date !== undefined && date !== null && date !== '') {
    if (!isValidISODate(date)) return res.status(400).json({ error: 'التاريخ غير صالح (الصيغة YYYY-MM-DD)' });
    params.push(date); setSql = 'purchase_date = $3::date';
  } else if (Number.isInteger(Number(shiftDays)) && Number(shiftDays) !== 0 && Math.abs(Number(shiftDays)) <= 3660) {
    params.push(Number(shiftDays)); setSql = 'purchase_date = (purchase_date + $3::int)';
  } else {
    return res.status(400).json({ error: 'حدد تاريخًا جديدًا أو عدد أيام للإزاحة' });
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const upd = await client.query(`UPDATE company_assets SET ${setSql} WHERE id = ANY($1::int[]) AND company_id = $2`, params);
    await client.query(
      `UPDATE company_expenses e SET entry_date = a.purchase_date
       FROM company_assets a WHERE e.asset_id = a.id AND a.id = ANY($1::int[]) AND a.company_id = $2`, [idList, cid]);
    await client.query('COMMIT');
    res.json({ updated: upd.rowCount });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'تعذر تعديل التواريخ: ' + e.message });
  } finally { client.release(); }
});

// Same useful life (in years) for many assets at once; depreciation and book value are worked out from it on the fly.
app.post('/api/companies/:id/assets/bulk-life', auth, requireAdmin, async (req, res) => {
  const { ids, years } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد أصول محددة' });
  const y = Number(years);
  if (!Number.isFinite(y) || y <= 0 || y > 200) return res.status(400).json({ error: 'العمر الافتراضي لازم يكون عدد سنين أكبر من صفر' });
  const idList = ids.map(n => parseInt(n, 10)).filter(Boolean);
  const upd = await pool.query('UPDATE company_assets SET useful_life_years=$1 WHERE id = ANY($2::int[]) AND company_id=$3', [y, idList, req.params.id]);
  res.json({ updated: upd.rowCount });
});

// Excel import. Rows that are already in the register (same name + date + cost) are skipped, so importing the same
// file twice can't duplicate anything. recordExpenses (default true) books each new asset as a "شراء أصول" expense.
app.post('/api/companies/:id/assets/bulk', auth, requireAdmin, async (req, res) => {
  const { rows, recordExpenses } = req.body || {};
  if (!Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  const valid = rows.filter(r => r.name && Number(r.cost) > 0 && r.purchaseDate && isValidISODate(r.purchaseDate));
  if (!valid.length) return res.status(400).json({ error: 'لا يوجد صفوف صالحة للاستيراد (تحقق من صيغة التاريخ)' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const key = (n, d, c) => String(n).trim().toLowerCase().replace(/\s+/g, ' ') + '|' + d + '|' + Number(c).toFixed(2);
    const have = {};
    (await client.query('SELECT name, purchase_date::text AS d, cost FROM company_assets WHERE company_id=$1', [req.params.id]))
      .rows.forEach(r => { const k = key(r.name, r.d, r.cost); have[k] = (have[k] || 0) + 1; });
    const fresh = [];
    let skipped = 0;
    valid.forEach(r => {
      const k = key(r.name, r.purchaseDate, r.cost);
      if (have[k] > 0) { have[k]--; skipped++; } else fresh.push(r);
    });
    let ids = [];
    if (fresh.length) {
      const ins = await client.query(
        `INSERT INTO company_assets (company_id, name, category, purchase_date, cost, useful_life_years, notes)
         SELECT $1, u.name, u.category, u.purchase_date, u.cost, u.useful_life_years, u.notes
         FROM UNNEST($2::text[], $3::text[], $4::date[], $5::numeric[], $6::numeric[], $7::text[])
           AS u(name, category, purchase_date, cost, useful_life_years, notes)
         RETURNING id`,
        [req.params.id, fresh.map(r => r.name), fresh.map(r => r.category || null),
         fresh.map(r => r.purchaseDate), fresh.map(r => r.cost),
         fresh.map(r => r.usefulLifeYears || null), fresh.map(r => r.notes || null)]
      );
      ids = ins.rows.map(r => r.id);
    }
    const expenses = (recordExpenses !== false && ids.length) ? await recordAssetPurchases(client, req.params.id, ids) : 0;
    await client.query('COMMIT');
    res.json({ inserted: ids.length, skipped, expenses });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('assets bulk import failed', e);
    res.status(500).json({ error: 'فشل الاستيراد: ' + e.message });
  } finally { client.release(); }
});

// Aggregate report: company's own general expenses + costs and revenue of every project under it
app.get('/api/companies/:id/report', auth, requireAdmin, async (req, res) => {
  const companyId = req.params.id;
  const assetRows = (await pool.query('SELECT * FROM company_assets WHERE company_id=$1', [companyId])).rows.map(withAssetDepreciation);
  const totalAssetsCost = assetRows.reduce((s, a) => s + a.cost, 0);
  const totalAssetsBookValue = assetRows.reduce((s, a) => s + a.bookValue, 0);
  const companyExpRows = (await pool.query(
    'SELECT category, COALESCE(SUM(amount),0) AS t FROM company_expenses WHERE company_id=$1 GROUP BY category',
    [companyId]
  )).rows;
  const companyExpenses = companyExpRows.reduce((s, r) => s + Number(r.t), 0);
  const companyExpensesByCategory = companyExpRows.map(r => ({ category: r.category || 'أخرى', total: Number(r.t) }));

  const projRows = (await pool.query(`
    SELECT pr.id, pr.name,
      COALESCE(SUM(CASE WHEN e.kind='revenue' THEN e.amount ELSE 0 END),0) AS revenue,
      COALESCE(SUM(CASE WHEN e.kind='expense' THEN e.amount ELSE 0 END),0) AS expense
    FROM projects pr
    LEFT JOIN entries e ON e.project_id = pr.id
    WHERE pr.company_id = $1
    GROUP BY pr.id, pr.name
    ORDER BY pr.id
  `, [companyId])).rows;
  // "المحصل من العملاء" = every collection recorded against the sales of this company's projects
  const collectedByProject = {};
  (await pool.query(`
    SELECT s.project_id, COALESCE(SUM(c.amount), 0) AS t
    FROM sale_collections c JOIN inventory_sales s ON s.id = c.sale_id JOIN projects p ON p.id = s.project_id
    WHERE p.company_id = $1 GROUP BY s.project_id
  `, [companyId])).rows.forEach(r => { collectedByProject[r.project_id] = Number(r.t); });
  const projects = projRows.map(p => ({
    id: p.id, name: p.name, revenue: Number(p.revenue), expense: Number(p.expense), net: Number(p.revenue) - Number(p.expense),
    collected: collectedByProject[p.id] || 0
  }));
  const totalCollected = projects.reduce((s, p) => s + p.collected, 0);
  const totalProjectRevenue = projects.reduce((s, p) => s + p.revenue, 0);
  const totalProjectExpense = projects.reduce((s, p) => s + p.expense, 0);
  const grandNet = totalProjectRevenue - (companyExpenses + totalProjectExpense);

  const shareRows = (await pool.query(`
    SELECT cp.partner_id, cp.percentage, cp.opening_balance, p.name
    FROM company_partners cp JOIN partners p ON p.id = cp.partner_id
    WHERE cp.company_id = $1
  `, [companyId])).rows;
  const caRows = (await pool.query(`
    SELECT partner_id,
      COALESCE(SUM(CASE WHEN kind IN ('deposit','distribution') THEN amount ELSE -amount END),0) AS balance
    FROM current_account WHERE company_id=$1 GROUP BY partner_id
  `, [companyId])).rows;
  const caMap = {};
  caRows.forEach(r => { caMap[r.partner_id] = Number(r.balance); });
  const grandTotalExpenses = companyExpenses + totalProjectExpense;
  const partners = shareRows.map(s => ({
    partnerId: s.partner_id,
    name: s.name,
    percentage: Number(s.percentage),
    openingBalance: Number(s.opening_balance),
    shareOfRevenue: totalProjectRevenue * (Number(s.percentage) / 100),
    shareOfExpenses: grandTotalExpenses * (Number(s.percentage) / 100),
    shareOfCollected: totalCollected * (Number(s.percentage) / 100),
    // > 0: his share of what customers paid covers his share of the expenses with a surplus ("له");
    // < 0: it doesn't cover it, so that much is still on him ("عليه")
    settlement: (totalCollected - grandTotalExpenses) * (Number(s.percentage) / 100),
    shareOfNet: grandNet * (Number(s.percentage) / 100),
    currentAccountBalance: (caMap[s.partner_id] || 0) + Number(s.opening_balance)
  }));

  res.json({
    companyId: Number(companyId),
    companyExpenses,
    companyExpensesByCategory,
    projects,
    partners,
    totalProjectRevenue,
    totalProjectExpense,
    grandTotalExpenses,
    totalCollected,
    collectedMinusExpenses: totalCollected - grandTotalExpenses,
    grandNet,
    totalAssetsCost,
    totalAssetsBookValue
  });
});

// ---------------------------------------------------------------------------
// Admin: projects
// ---------------------------------------------------------------------------
app.get('/api/projects', auth, requireAdmin, async (req, res) => {
  const projects = (await pool.query(`
    SELECT pr.id, pr.name, pr.company_id, c.name AS company_name
    FROM projects pr LEFT JOIN companies c ON c.id = pr.company_id
    ORDER BY pr.id
  `)).rows;
  const shares = (await pool.query(`
    SELECT pp.project_id, pp.partner_id, pp.percentage, pp.opening_balance, p.name AS partner_name
    FROM project_partners pp JOIN partners p ON p.id = pp.partner_id
  `)).rows;
  const withShares = projects.map(pr => ({
    id: pr.id, name: pr.name, companyId: pr.company_id, companyName: pr.company_name,
    partners: shares.filter(s => s.project_id === pr.id)
      .map(s => ({ partnerId: s.partner_id, percentage: Number(s.percentage), openingBalance: Number(s.opening_balance), name: s.partner_name }))
  }));
  res.json(withShares);
});

app.post('/api/projects', auth, requireAdmin, async (req, res) => {
  const { name, shares, companyId } = req.body || {};
  if (!name || !Array.isArray(shares) || shares.length === 0) {
    return res.status(400).json({ error: 'اسم المشروع والشركاء مطلوبون' });
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const projRes = await client.query('INSERT INTO projects (name, company_id) VALUES ($1,$2) RETURNING id', [name, companyId || null]);
    const projectId = projRes.rows[0].id;
    for (const s of shares) {
      await client.query(
        'INSERT INTO project_partners (project_id, partner_id, percentage) VALUES ($1,$2,$3)',
        [projectId, s.partnerId, s.percentage]
      );
    }
    await client.query('COMMIT');
    res.json({ id: projectId, name });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'حدث خطأ أثناء إنشاء المشروع' });
  } finally {
    client.release();
  }
});

app.put('/api/projects/:id', auth, requireAdmin, async (req, res) => {
  const { companyId } = req.body || {};
  const { rows } = await pool.query('UPDATE projects SET company_id=$1 WHERE id=$2 RETURNING *', [companyId || null, req.params.id]);
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

app.delete('/api/projects/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM projects WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

// Add a partner to an existing project
app.post('/api/projects/:id/partners', auth, requireAdmin, async (req, res) => {
  const { partnerId, percentage, openingBalance } = req.body || {};
  if (!partnerId || percentage === undefined) return res.status(400).json({ error: 'بيانات ناقصة' });
  try {
    const { rows } = await pool.query(
      'INSERT INTO project_partners (project_id, partner_id, percentage, opening_balance) VALUES ($1,$2,$3,$4) RETURNING *',
      [req.params.id, partnerId, percentage, openingBalance || 0]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(409).json({ error: 'هذا الشريك مضاف بالفعل لهذا المشروع' });
  }
});

// Update a partner's percentage and/or opening balance within a project
app.put('/api/projects/:id/partners/:partnerId', auth, requireAdmin, async (req, res) => {
  const { percentage, openingBalance } = req.body || {};
  const { rows } = await pool.query(
    `UPDATE project_partners SET
       percentage = COALESCE($1, percentage),
       opening_balance = COALESCE($2, opening_balance)
     WHERE project_id=$3 AND partner_id=$4 RETURNING *`,
    [percentage === undefined ? null : percentage, openingBalance === undefined ? null : openingBalance, req.params.id, req.params.partnerId]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

// Remove a partner from a project (does not delete the partner themselves)
app.delete('/api/projects/:id/partners/:partnerId', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM project_partners WHERE project_id=$1 AND partner_id=$2', [req.params.id, req.params.partnerId]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Entries (revenue/expense) — admin writes, both roles can read (scoped)
// ---------------------------------------------------------------------------
async function assertProjectAccess(req, res, projectId) {
  if (req.user.role === 'admin') return true;
  const { rows } = await pool.query(
    'SELECT 1 FROM project_partners WHERE project_id=$1 AND partner_id=$2',
    [projectId, req.user.partnerId]
  );
  if (!rows.length) {
    res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذا المشروع' });
    return false;
  }
  return true;
}

async function assertCompanyAccess(req, res, companyId) {
  if (req.user.role === 'admin') return true;
  const { rows } = await pool.query(
    'SELECT 1 FROM company_partners WHERE company_id=$1 AND partner_id=$2',
    [companyId, req.user.partnerId]
  );
  if (!rows.length) {
    res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذه الشركة' });
    return false;
  }
  return true;
}

app.get('/api/entries', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(
    'SELECT * FROM entries WHERE project_id=$1 ORDER BY entry_date DESC, created_at DESC',
    [projectId]
  );
  if (req.user.role === 'partner') {
    // partners see expenses by main item only (via /api/me/expense-summary), never the line-level detail
    return res.json(rows.map(r => (r.kind === 'expense' ? { ...r, description: null, category: null } : r)));
  }
  res.json(rows);
});

app.post('/api/entries', auth, requireAdmin, async (req, res) => {
  const { projectId, kind, amount, description, date, category } = req.body || {};
  if (!projectId || !['revenue', 'expense'].includes(kind) || !amount || !date) {
    return res.status(400).json({ error: 'بيانات ناقصة' });
  }
  const { rows } = await pool.query(
    'INSERT INTO entries (project_id, kind, amount, description, entry_date, category) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
    [projectId, kind, amount, description || null, date, kind === 'expense' ? (category || 'أخرى') : null]
  );
  res.json(rows[0]);
});

app.put('/api/entries/:id', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const bad = checkMoneyEdit(b);
  if (bad) return res.status(400).json({ error: bad });
  const v = (x) => (x === undefined ? null : x);
  const { rows } = await pool.query(
    `UPDATE entries SET
       amount = COALESCE($1, amount),
       entry_date = COALESCE($2, entry_date),
       description = COALESCE($3, description),
       category = CASE WHEN kind = 'expense' THEN COALESCE($4, category) ELSE NULL END
     WHERE id=$5 RETURNING *`,
    [v(b.amount), v(b.date), v(b.description), v(b.category), req.params.id]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});
registerBulkDate('/api/entries/bulk-date', 'entries', 'entry_date');

app.delete('/api/entries/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM entries WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/api/entries/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  await pool.query('DELETE FROM entries WHERE id = ANY($1::int[])', [ids]);
  res.json({ ok: true, deleted: ids.length });
});

// ---------------------------------------------------------------------------
// Expense payments (تحصيل/سداد المصروفات — المتبقي من كل مصروف)
// ---------------------------------------------------------------------------
app.get('/api/expense-payments', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(`
    SELECT ep.* FROM expense_payments ep
    JOIN entries e ON e.id = ep.entry_id
    WHERE e.project_id = $1
    ORDER BY ep.payment_date DESC, ep.created_at DESC
  `, [projectId]);
  res.json(req.user.role === 'partner' ? rows.map(r => ({ ...r, description: null })) : rows);
});

app.post('/api/expense-payments', auth, requireAdmin, async (req, res) => {
  const { entryId, amount, date, description } = req.body || {};
  if (!entryId || !amount || !date) return res.status(400).json({ error: 'بيانات ناقصة' });
  const { rows } = await pool.query(
    'INSERT INTO expense_payments (entry_id, amount, payment_date, description) VALUES ($1,$2,$3,$4) RETURNING *',
    [entryId, amount, date, description || null]
  );
  res.json(rows[0]);
});

// Settle many expenses at once: every selected expense that still has a balance gets ONE payment
// for exactly its remaining amount. One statement, so it either applies fully or not at all, and a
// double click can't pay twice (the second run finds nothing left to pay). Revenue rows, refunds and
// already-settled expenses are skipped.
app.post('/api/expense-payments/bulk', auth, requireAdmin, async (req, res) => {
  const { entryIds, date, description } = req.body || {};
  if (!Array.isArray(entryIds) || !entryIds.length) return res.status(400).json({ error: 'لم يتم تحديد أي مصروف' });
  if (!date || !isValidISODate(date)) return res.status(400).json({ error: 'تاريخ السداد غير صحيح' });
  const ids = [...new Set(entryIds.map(n => parseInt(n, 10)).filter(Boolean))];
  if (!ids.length) return res.status(400).json({ error: 'لم يتم تحديد أي مصروف' });
  try {
    const { rows } = await pool.query(`
      INSERT INTO expense_payments (entry_id, amount, payment_date, description)
      SELECT e.id, e.amount - COALESCE(p.paid, 0), $2::date, $3
      FROM entries e
      LEFT JOIN (
        SELECT entry_id, SUM(amount) AS paid FROM expense_payments WHERE entry_id = ANY($1::int[]) GROUP BY entry_id
      ) p ON p.entry_id = e.id
      WHERE e.id = ANY($1::int[]) AND e.kind = 'expense' AND e.amount - COALESCE(p.paid, 0) > 0.005
      RETURNING entry_id, amount
    `, [ids, date, description || 'سداد جماعي']);
    const totalPaid = rows.reduce((t, r) => t + Number(r.amount), 0);
    res.json({ paid: rows.length, totalPaid, skipped: ids.length - rows.length });
  } catch (e) {
    console.error('bulk expense payment failed', e);
    res.status(500).json({ error: 'تعذر تسجيل السداد: ' + e.message });
  }
});

app.delete('/api/expense-payments/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM expense_payments WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Inventory: items, sales, and collections per project
// ---------------------------------------------------------------------------
app.get('/api/inventory/items', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(
    'SELECT * FROM inventory_items WHERE project_id=$1 ORDER BY id',
    [projectId]
  );
  res.json(rows);
});

app.post('/api/inventory/items', auth, requireAdmin, async (req, res) => {
  const { projectId, name, unit, quantityIn, unitPrice, status, netArea, gardenArea, building } = req.body || {};
  if (!projectId || !name || quantityIn === undefined) return res.status(400).json({ error: 'بيانات ناقصة' });
  const { rows } = await pool.query(
    'INSERT INTO inventory_items (project_id, name, unit, quantity_in, unit_price, status, net_area, garden_area, building) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *',
    [projectId, name, unit || null, quantityIn, unitPrice || 0, status || null, netArea || null, gardenArea || null, building || null]
  );
  res.json(rows[0]);
});

// Edit an item's status / price / area / building (name, unit, quantity stay fixed here to avoid
// silently breaking sale history; delete+recreate if those truly need to change)
// Change the status of many items at once ("" clears it). Units that already have sales keep whatever you set here until a
// sale on them is added / edited / deleted, which re-applies the usual rule (sold out = "مباع").
app.post('/api/inventory/items/bulk-status', auth, requireAdmin, async (req, res) => {
  const { ids, status } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد أصناف محددة' });
  if (typeof status !== 'string' || status.trim().length > 40) return res.status(400).json({ error: 'الحالة غير صحيحة' });
  const idList = ids.map(n => parseInt(n, 10)).filter(Boolean);
  const upd = await pool.query('UPDATE inventory_items SET status=$1 WHERE id = ANY($2::int[])', [status.trim(), idList]);
  res.json({ updated: upd.rowCount });
});

app.put('/api/inventory/items/:id', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const v = (x) => (x === undefined ? null : x);
  if (b.name !== undefined && !String(b.name).trim()) return res.status(400).json({ error: 'اسم الصنف مطلوب' });
  if (b.quantityIn !== undefined && b.quantityIn !== null) {
    const q = Number(b.quantityIn);
    if (!Number.isFinite(q) || q < 0) return res.status(400).json({ error: 'الكمية غير صحيحة' });
    const sold = Number((await pool.query('SELECT COALESCE(SUM(quantity),0) AS q FROM inventory_sales WHERE item_id=$1', [req.params.id])).rows[0].q);
    if (q + 1e-9 < sold) return res.status(400).json({ error: 'الكمية أقل من اللي اتباع منه فعلًا (' + sold + ')' });
  }
  const { rows } = await pool.query(
    `UPDATE inventory_items SET
       name = COALESCE($1, name),
       unit = COALESCE($2, unit),
       quantity_in = COALESCE($3, quantity_in),
       status = COALESCE($4, status),
       unit_price = COALESCE($5, unit_price),
       net_area = COALESCE($6, net_area),
       garden_area = COALESCE($7, garden_area),
       building = COALESCE($8, building)
     WHERE id=$9 RETURNING *`,
    [b.name === undefined ? null : String(b.name).trim(), v(b.unit), v(b.quantityIn), v(b.status), v(b.unitPrice), v(b.netArea), v(b.gardenArea), v(b.building), req.params.id]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  await syncSoldStatus(pool, [rows[0].id]);   // a quantity change can make the unit sold-out (or free again)
  res.json((await pool.query('SELECT * FROM inventory_items WHERE id=$1', [rows[0].id])).rows[0]);   // re-read so the status is the synced one
});

app.delete('/api/inventory/items/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM inventory_items WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/api/inventory/items/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  await pool.query('DELETE FROM inventory_items WHERE id = ANY($1::int[])', [ids]);
  res.json({ ok: true, deleted: ids.length });
});

// Keeps a unit's status in step with its sales: once everything in stock is sold the unit becomes "مباع";
// if a sale is deleted/reduced and stock comes back, a unit that was "مباع" goes back to "متاح".
// Any other status ("محجوز", "مغلق", custom) is never touched while stock remains.
async function syncSoldStatus(db, itemIds) {
  const ids = [...new Set((itemIds || []).map(n => parseInt(n, 10)).filter(Boolean))];
  if (!ids.length) return;
  await db.query(`
    UPDATE inventory_items i SET status = CASE
        WHEN s.q IS NOT NULL AND i.quantity_in - s.q <= 0.000001 THEN 'مباع'
        WHEN i.status = 'مباع' THEN 'متاح'
        ELSE i.status END
    FROM (
      SELECT x.id, (SELECT SUM(quantity) FROM inventory_sales WHERE item_id = x.id) AS q
      FROM inventory_items x WHERE x.id = ANY($1::int[])
    ) s
    WHERE i.id = s.id
  `, [ids]);
}

app.get('/api/inventory/sales', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(`
    SELECT s.*, i.name AS item_name, i.unit AS item_unit, i.net_area AS item_net_area, i.garden_area AS item_garden_area, i.building AS item_building, i.unit_price AS item_unit_price
    FROM inventory_sales s JOIN inventory_items i ON i.id = s.item_id
    WHERE s.project_id=$1 ORDER BY s.sale_date DESC, s.created_at DESC
  `, [projectId]);
  // partners only ever see the price a unit was sold at, never how it compares to the inventory price
  if (req.user.role === 'partner') return res.json(rows.map(({ item_unit_price, ...rest }) => rest));
  res.json(rows);
});

app.post('/api/inventory/sales', auth, requireAdmin, async (req, res) => {
  const {
    projectId, itemId, quantity, saleAmount, saleDate, description,
    customerName, buildingNo, garageValue, garageCollected,
    maintenanceValue, maintenanceCollected, utilitiesValue, utilitiesCollected,
    bankCollected, bankHeld, collectionDiff, downPaymentPercent
  } = req.body || {};
  if (!projectId || !itemId || !quantity || !saleAmount || !saleDate) {
    return res.status(400).json({ error: 'بيانات ناقصة' });
  }
  const { rows } = await pool.query(
    `INSERT INTO inventory_sales
      (project_id, item_id, quantity, sale_amount, sale_date, description,
       customer_name, building_no, garage_value, garage_collected,
       maintenance_value, maintenance_collected, utilities_value, utilities_collected,
       bank_collected, bank_held, collection_diff, down_payment_percent)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING *`,
    [projectId, itemId, quantity, saleAmount, saleDate, description || null,
     customerName || null, buildingNo || null, garageValue || 0, garageCollected || 0,
     maintenanceValue || 0, maintenanceCollected || 0, utilitiesValue || 0, utilitiesCollected || 0,
     bankCollected || 0, bankHeld || 0, collectionDiff || 0, downPaymentPercent === undefined ? null : downPaymentPercent]
  );
  await syncSoldStatus(pool, [rows[0].item_id]);
  res.json(rows[0]);
});

// Import many sales for ONE project in a single transaction. For every row the server re-checks what the
// browser already previewed: the unit belongs to the project, the date is valid, there is stock left
// (so importing the same sheet twice can't sell a unit twice), then it creates the sale and, when a
// collected amount is given, one dated collection entry for it.
app.post('/api/inventory/sales/bulk', auth, requireAdmin, async (req, res) => {
  const { projectId, rows } = req.body || {};
  const pid = parseInt(projectId, 10);
  if (!pid || !Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  if (rows.length > 3000) return res.status(400).json({ error: 'الملف كبير جدًا (الحد 3000 صف في المرة)' });
  const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const items = (await client.query('SELECT id, quantity_in FROM inventory_items WHERE project_id=$1', [pid])).rows;
    if (!items.length) { await client.query('ROLLBACK'); return res.status(400).json({ error: 'المشروع ده مفيهوش مخزون' }); }
    const left = {};
    items.forEach(i => { left[i.id] = Number(i.quantity_in); });
    (await client.query('SELECT item_id, SUM(quantity) AS q FROM inventory_sales WHERE project_id=$1 GROUP BY item_id', [pid]))
      .rows.forEach(r => { if (r.item_id in left) left[r.item_id] -= Number(r.q); });

    let inserted = 0, collections = 0;
    const rejected = [], touched = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const itemId = parseInt(r.itemId, 10), qty = r.quantity === undefined ? 1 : num(r.quantity), amount = num(r.saleAmount);
      let reason = null;
      if (!(itemId in left)) reason = 'الوحدة مش تابعة للمشروع ده';
      else if (!r.saleDate || !isValidISODate(r.saleDate)) reason = 'تاريخ التعاقد غير صالح';
      else if (!(amount > 0)) reason = 'قيمة الوحدة غير صالحة';
      else if (!(qty > 0)) reason = 'الكمية غير صالحة';
      else if (left[itemId] + 1e-9 < qty) reason = 'الوحدة مباعة بالفعل (مفيش متبقي منها)';
      else if (r.downPaymentPercent != null && !(num(r.downPaymentPercent) >= 0 && num(r.downPaymentPercent) <= 100)) reason = 'نسبة المقدم غير منطقية';
      if (reason) { rejected.push({ index: i, reason }); continue; }

      const ins = await client.query(
        `INSERT INTO inventory_sales
          (project_id, item_id, quantity, sale_amount, sale_date, description, customer_name, building_no,
           garage_value, garage_collected, maintenance_value, maintenance_collected, utilities_value, utilities_collected,
           collection_diff, down_payment_percent)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id`,
        [pid, itemId, qty, amount, r.saleDate, r.description || null, r.customerName || null, r.buildingNo || null,
         num(r.garageValue), num(r.garageCollected), num(r.maintenanceValue), num(r.maintenanceCollected),
         num(r.utilitiesValue), num(r.utilitiesCollected), num(r.collectionDiff),
         r.downPaymentPercent == null ? null : num(r.downPaymentPercent)]
      );
      left[itemId] -= qty;
      touched.push(itemId);
      inserted++;
      const collected = num(r.collected);
      if (collected > 0) {
        const cdate = (r.collectionDate && isValidISODate(r.collectionDate)) ? r.collectionDate : r.saleDate;
        await client.query(
          'INSERT INTO sale_collections (sale_id, amount, collection_date, description) VALUES ($1,$2,$3,$4)',
          [ins.rows[0].id, collected, cdate, 'تحصيل مستورد من إكسيل']
        );
        collections++;
      }
    }
    await syncSoldStatus(client, touched);
    await client.query('COMMIT');
    res.json({ inserted, collections, rejected });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('sales bulk import failed', e);
    res.status(500).json({ error: 'فشل استيراد المبيعات: ' + e.message });
  } finally {
    client.release();
  }
});

// One-click catch-up for units sold BEFORE statuses followed sales: every fully-sold unit in the project that
// isn't marked "مباع" yet gets marked. Only moves units forward to "مباع"; nothing else is changed.
app.post('/api/inventory/sync-status', auth, requireAdmin, async (req, res) => {
  const pid = parseInt((req.body || {}).projectId, 10);
  if (!pid) return res.status(400).json({ error: 'projectId مطلوب' });
  const { rows } = await pool.query(`
    UPDATE inventory_items i SET status = 'مباع'
    WHERE i.project_id = $1 AND COALESCE(i.status, '') <> 'مباع'
      AND EXISTS (SELECT 1 FROM inventory_sales s WHERE s.item_id = i.id)
      AND i.quantity_in - (SELECT SUM(quantity) FROM inventory_sales s WHERE s.item_id = i.id) <= 0.000001
    RETURNING i.id
  `, [pid]);
  res.json({ updated: rows.length });
});

// Make the inventory price equal to the price a unit was actually sold at (per unit = sale value / quantity).
// Only units sold at ONE consistent price are changed; a unit sold at several different prices is skipped and
// reported, because there is no single "right" price for it. Pass itemIds to fix specific units only.
app.post('/api/inventory/sync-prices', auth, requireAdmin, async (req, res) => {
  const pid = parseInt((req.body || {}).projectId, 10);
  if (!pid) return res.status(400).json({ error: 'projectId مطلوب' });
  const only = Array.isArray((req.body || {}).itemIds) ? req.body.itemIds.map(n => parseInt(n, 10)).filter(Boolean) : null;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(`
      SELECT i.id, i.name, i.unit_price,
             COUNT(DISTINCT ROUND(s.sale_amount / NULLIF(s.quantity, 0), 2)) AS prices,
             MIN(ROUND(s.sale_amount / NULLIF(s.quantity, 0), 2)) AS price
      FROM inventory_items i JOIN inventory_sales s ON s.item_id = i.id
      WHERE i.project_id = $1 AND ($2::int[] IS NULL OR i.id = ANY($2::int[]))
      GROUP BY i.id, i.name, i.unit_price
    `, [pid, only]);
    const changes = [], skipped = [];
    for (const r of rows) {
      if (Number(r.prices) !== 1) { skipped.push({ itemId: r.id, name: r.name, reason: 'مباعة بأكتر من سعر' }); continue; }
      const from = Number(r.unit_price), to = Number(r.price);
      if (!(to > 0) || Math.abs(from - to) <= 0.005) continue;
      await client.query('UPDATE inventory_items SET unit_price=$1 WHERE id=$2', [to, r.id]);
      changes.push({ itemId: r.id, name: r.name, from, to });
    }
    await client.query('COMMIT');
    res.json({ updated: changes.length, changes, skipped });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('sync prices failed', e);
    res.status(500).json({ error: 'تعذر تحديث الأسعار: ' + e.message });
  } finally {
    client.release();
  }
});

app.put('/api/inventory/sales/:id', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const v = (x) => (x === undefined ? null : x);
  const cur = (await pool.query('SELECT id, project_id, item_id, quantity FROM inventory_sales WHERE id=$1', [req.params.id])).rows[0];
  if (!cur) return res.status(404).json({ error: 'غير موجود' });
  const bad = checkMoneyEdit({ amount: b.saleAmount, date: b.saleDate });
  if (bad) return res.status(400).json({ error: bad });
  const newItem = b.itemId ? parseInt(b.itemId, 10) : cur.item_id;
  const newQty = b.quantity !== undefined && b.quantity !== null ? Number(b.quantity) : Number(cur.quantity);
  if (!(newQty > 0)) return res.status(400).json({ error: 'الكمية غير صحيحة' });
  if (newItem !== cur.item_id || newQty !== Number(cur.quantity)) {
    // the unit must belong to the same project and still have enough in stock once this sale is counted in it
    const it = (await pool.query('SELECT id, quantity_in FROM inventory_items WHERE id=$1 AND project_id=$2', [newItem, cur.project_id])).rows[0];
    if (!it) return res.status(400).json({ error: 'الوحدة دي مش تابعة للمشروع' });
    const others = Number((await pool.query('SELECT COALESCE(SUM(quantity),0) AS q FROM inventory_sales WHERE item_id=$1 AND id<>$2', [newItem, cur.id])).rows[0].q);
    if (others + newQty > Number(it.quantity_in) + 1e-9) return res.status(400).json({ error: 'الوحدة دي مفيهاش مخزون كفاية (المتبقي ' + (Number(it.quantity_in) - others) + ')' });
  }
  const { rows } = await pool.query(
    `UPDATE inventory_sales SET
       item_id = $18,
       quantity = COALESCE($1, quantity),
       sale_amount = COALESCE($2, sale_amount),
       sale_date = COALESCE($3, sale_date),
       description = COALESCE($4, description),
       customer_name = COALESCE($5, customer_name),
       building_no = COALESCE($6, building_no),
       garage_value = COALESCE($7, garage_value),
       garage_collected = COALESCE($8, garage_collected),
       maintenance_value = COALESCE($9, maintenance_value),
       maintenance_collected = COALESCE($10, maintenance_collected),
       utilities_value = COALESCE($11, utilities_value),
       utilities_collected = COALESCE($12, utilities_collected),
       bank_collected = COALESCE($13, bank_collected),
       bank_held = COALESCE($14, bank_held),
       collection_diff = COALESCE($15, collection_diff),
       down_payment_percent = COALESCE($16, down_payment_percent)
     WHERE id=$17 RETURNING *`,
    [v(b.quantity), v(b.saleAmount), v(b.saleDate), v(b.description), v(b.customerName), v(b.buildingNo),
     v(b.garageValue), v(b.garageCollected), v(b.maintenanceValue), v(b.maintenanceCollected),
     v(b.utilitiesValue), v(b.utilitiesCollected), v(b.bankCollected), v(b.bankHeld), v(b.collectionDiff),
     v(b.downPaymentPercent), req.params.id, newItem]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  await syncSoldStatus(pool, [rows[0].item_id, cur.item_id]);     // both the new unit and the one it was moved away from
  res.json(rows[0]);
});
registerBulkDate('/api/inventory/sales/bulk-date', 'inventory_sales', 'sale_date');
registerBulkDate('/api/inventory/collections/bulk-date', 'sale_collections', 'collection_date');

app.delete('/api/inventory/sales/:id', auth, requireAdmin, async (req, res) => {
  const del = await pool.query('DELETE FROM inventory_sales WHERE id=$1 RETURNING item_id', [req.params.id]);
  await syncSoldStatus(pool, del.rows.map(r => r.item_id));
  res.json({ ok: true });
});

app.post('/api/inventory/sales/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  const del = await pool.query('DELETE FROM inventory_sales WHERE id = ANY($1::int[]) RETURNING item_id', [ids]);
  await syncSoldStatus(pool, del.rows.map(r => r.item_id));
  res.json({ ok: true, deleted: ids.length });
});

app.get('/api/inventory/collections', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(`
    SELECT c.* FROM sale_collections c
    JOIN inventory_sales s ON s.id = c.sale_id
    WHERE s.project_id=$1
    ORDER BY c.collection_date DESC, c.created_at DESC
  `, [projectId]);
  res.json(rows);
});

app.post('/api/inventory/collections', auth, requireAdmin, async (req, res) => {
  const { saleId, amount, date, description } = req.body || {};
  if (!saleId || !amount || !date) return res.status(400).json({ error: 'بيانات ناقصة' });
  const bad = checkMoneyEdit({ amount, date });
  if (bad) return res.status(400).json({ error: bad });
  if (!(await pool.query('SELECT 1 FROM inventory_sales WHERE id=$1', [saleId])).rows.length) return res.status(404).json({ error: 'عملية البيع غير موجودة' });
  const { rows } = await pool.query(
    'INSERT INTO sale_collections (sale_id, amount, collection_date, description) VALUES ($1,$2,$3,$4) RETURNING *',
    [saleId, amount, date, description || null]
  );
  res.json(rows[0]);
});

app.put('/api/inventory/collections/:id', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const v = (x) => (x === undefined ? null : x);
  const { rows } = await pool.query(
    `UPDATE sale_collections SET
       amount = COALESCE($1, amount),
       collection_date = COALESCE($2, collection_date),
       description = COALESCE($3, description)
     WHERE id=$4 RETURNING *`,
    [v(b.amount), v(b.date), v(b.description), req.params.id]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

app.delete('/api/inventory/collections/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM sale_collections WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/api/inventory/collections/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  await pool.query('DELETE FROM sale_collections WHERE id = ANY($1::int[])', [ids]);
  res.json({ ok: true, deleted: ids.length });
});

// ---------------------------------------------------------------------------
// Current account
// ---------------------------------------------------------------------------

// Monthly statement (كشف حساب): مدين (withdrawals) / دائن (deposits+distributions) / رصيد آخر كل شهر.
// Reconciles with the totals shown for the same partner+company in the period analysis.
app.get('/api/current-account/ledger', auth, async (req, res) => {
  const companyId = req.query.companyId;
  if (!companyId) return res.status(400).json({ error: 'companyId مطلوب' });
  if (!(await assertCompanyAccess(req, res, companyId))) return;

  // Admins may ask for the combined ledger of every partner at once (aggregate = "مجمع").
  // Partners always see their own detailed ("تفصيلي") ledger only.
  const aggregate = req.user.role === 'admin' && req.query.partnerId === 'all';
  const partnerId = aggregate ? null : (req.user.role === 'admin' ? req.query.partnerId : req.user.partnerId);
  let year = parseInt(req.query.year, 10);
  if (!aggregate && !partnerId) return res.status(400).json({ error: 'partnerId مطلوب' });

  const partnerFilter = aggregate ? '' : ' AND partner_id=$2';
  const partnerParam = aggregate ? [companyId] : [companyId, partnerId];

  let openingBalanceEver;
  if (aggregate) {
    openingBalanceEver = Number((await pool.query(
      'SELECT COALESCE(SUM(opening_balance),0) AS t FROM company_partners WHERE company_id=$1', [companyId]
    )).rows[0].t);
  } else {
    const cp = (await pool.query(
      'SELECT opening_balance FROM company_partners WHERE company_id=$1 AND partner_id=$2',
      [companyId, partnerId]
    )).rows[0];
    if (!cp) return res.status(404).json({ error: 'هذا الشريك ليس له نصيب في هذه الشركة' });
    openingBalanceEver = Number(cp.opening_balance);
  }

  const yearsRows = (await pool.query(
    `SELECT DISTINCT EXTRACT(YEAR FROM entry_date)::int AS y FROM current_account WHERE company_id=$1${partnerFilter} ORDER BY y`,
    partnerParam
  )).rows;
  const years = yearsRows.map(r => r.y);
  if (!year) year = years.length ? years[years.length - 1] : new Date().getFullYear();

  const priorRow = (await pool.query(`
    SELECT COALESCE(SUM(CASE WHEN kind IN ('deposit','distribution') THEN amount ELSE -amount END), 0) AS t
    FROM current_account WHERE company_id=$1${partnerFilter} AND entry_date < (($${partnerParam.length + 1}) || '-01-01')::date
  `, [...partnerParam, year])).rows[0];
  const openingBalanceYear = openingBalanceEver + Number(priorRow.t);

  const monthRows = (await pool.query(`
    SELECT EXTRACT(MONTH FROM entry_date)::int AS m,
      COALESCE(SUM(CASE WHEN kind = 'withdrawal' THEN amount ELSE 0 END), 0) AS debit,
      COALESCE(SUM(CASE WHEN kind IN ('deposit','distribution') THEN amount ELSE 0 END), 0) AS credit
    FROM current_account
    WHERE company_id=$1${partnerFilter} AND EXTRACT(YEAR FROM entry_date)::int = $${partnerParam.length + 1}
    GROUP BY m
  `, [...partnerParam, year])).rows;
  const byMonth = {};
  monthRows.forEach(r => { byMonth[r.m] = { debit: Number(r.debit), credit: Number(r.credit) }; });

  let running = openingBalanceYear;
  const months = [];
  for (let m = 1; m <= 12; m++) {
    const d = (byMonth[m] || { debit: 0, credit: 0 });
    running += d.credit - d.debit;
    months.push({ month: m, debit: d.debit, credit: d.credit, balance: running });
  }

  res.json({ companyId: Number(companyId), partnerId: aggregate ? null : Number(partnerId), aggregate, year, years,
    openingBalanceYear, closingBalanceYear: running, months });
});

app.get('/api/current-account', auth, async (req, res) => {
  const companyId = req.query.companyId;
  const partnerId = req.query.partnerId;
  if (!companyId) return res.status(400).json({ error: 'companyId مطلوب' });
  if (!(await assertCompanyAccess(req, res, companyId))) return;
  const effectivePartnerId = req.user.role === 'admin' ? partnerId : req.user.partnerId;
  let query = 'SELECT * FROM current_account WHERE company_id=$1';
  const params = [companyId];
  if (effectivePartnerId) {
    params.push(effectivePartnerId);
    query += ` AND partner_id=$${params.length}`;
  }
  query += ' ORDER BY entry_date DESC, created_at DESC';
  const { rows } = await pool.query(query, params);
  res.json(rows);
});

app.post('/api/current-account', auth, requireAdmin, async (req, res) => {
  const { companyId, partnerId, kind, amount, description, date } = req.body || {};
  if (!companyId || !partnerId || !['deposit', 'withdrawal', 'distribution'].includes(kind) || !amount || !date) {
    return res.status(400).json({ error: 'بيانات ناقصة' });
  }
  const { rows } = await pool.query(
    'INSERT INTO current_account (company_id, partner_id, kind, amount, description, entry_date) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
    [companyId, partnerId, kind, amount, description || null, date]
  );
  res.json(rows[0]);
});

app.put('/api/current-account/:id', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const bad = checkMoneyEdit(b);
  if (bad) return res.status(400).json({ error: bad });
  if (b.kind !== undefined && b.kind !== null && !['deposit', 'withdrawal', 'distribution'].includes(b.kind)) return res.status(400).json({ error: 'نوع الحركة غير صحيح' });
  const v = (x) => (x === undefined ? null : x);
  const { rows } = await pool.query(
    `UPDATE current_account SET amount = COALESCE($1, amount), entry_date = COALESCE($2, entry_date),
       description = COALESCE($3, description), kind = COALESCE($4, kind) WHERE id=$5 RETURNING *`,
    [v(b.amount), v(b.date), v(b.description), v(b.kind), req.params.id]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});
registerBulkDate('/api/current-account/bulk-date', 'current_account', 'entry_date');

app.delete('/api/current-account/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM current_account WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/api/current-account/bulk-delete', auth, requireAdmin, async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'لا يوجد عناصر محددة' });
  await pool.query('DELETE FROM current_account WHERE id = ANY($1::int[])', [ids]);
  res.json({ ok: true, deleted: ids.length });
});

// ---------------------------------------------------------------------------
// Partner: my projects + summary
// ---------------------------------------------------------------------------
app.get('/api/me/projects', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  const { rows } = await pool.query(`
    SELECT pr.id, pr.name, pp.percentage, pr.company_id AS "companyId", c.name AS "companyName"
    FROM projects pr
    JOIN project_partners pp ON pp.project_id = pr.id
    LEFT JOIN companies c ON c.id = pr.company_id
    WHERE pp.partner_id = $1
    ORDER BY pr.id
  `, [req.user.partnerId]);
  res.json(rows);
});

app.get('/api/me/companies', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  const { rows } = await pool.query(`
    SELECT c.id, c.name, cp.percentage
    FROM companies c
    JOIN company_partners cp ON cp.company_id = c.id
    WHERE cp.partner_id = $1
    ORDER BY c.id
  `, [req.user.partnerId]);
  res.json(rows);
});

// ---------------------------------------------------------------------------
// Partner-facing expense summary: MAIN items only. Level 1 = the chart's groups,
// level 2 = their direct items; anything deeper (individual clients, salary lines...)
// is rolled up into its level-2 item so the detail never reaches a partner's screen.
// Returns the totals plus the partner's own share (company % or project %).
// ---------------------------------------------------------------------------
const UNCLASSIFIED_LABEL = 'بنود أخرى غير مصنفة';

async function loadExpenseIndex() {
  const items = (await pool.query('SELECT id, name, parent_id FROM expense_items ORDER BY sort_order, id')).rows;
  const byId = {}, byNorm = {};
  items.forEach((it, idx) => { it.rank = idx; byId[it.id] = it; byNorm[normCat(it.name)] = it; });
  return { byId, byNorm };
}

async function rollupForPartner(categoryTotals, pct) {
  return rollupWith(await loadExpenseIndex(), categoryTotals, pct);
}

function rollupWith(index, categoryTotals, pct) {
  const { byId, byNorm } = index;
  const pathOf = (node) => {
    const path = []; let cur = node, guard = 0;
    while (cur && guard++ < 50) { path.unshift(cur); cur = cur.parent_id == null ? null : byId[cur.parent_id]; }
    return path;
  };
  const groups = {};
  categoryTotals.forEach(({ category, total }) => {
    const node = byNorm[normCat(category)];
    const path = node ? pathOf(node) : [];
    const root = path[0] || null, child = path[1] || null;
    const key = root ? 'r' + root.id : 'x';
    const g = groups[key] || (groups[key] = { name: root ? root.name : UNCLASSIFIED_LABEL, rank: root ? root.rank : 1e9, total: 0, direct: 0, children: {} });
    g.total += total;
    if (child) {
      const c = g.children[child.id] || (g.children[child.id] = { name: child.name, rank: child.rank, total: 0 });
      c.total += total;
    } else {
      g.direct += total;
    }
  });
  const nonZero = (n) => Math.abs(n) >= 0.005;
  const out = Object.values(groups).sort((a, b) => a.rank - b.rank).filter(g => nonZero(g.total)).map(g => ({
    name: g.name, total: g.total, myShare: g.total * pct,
    direct: g.direct, directShare: g.direct * pct,
    children: Object.values(g.children).sort((a, b) => a.rank - b.rank).filter(c => nonZero(c.total))
      .map(c => ({ name: c.name, total: c.total, myShare: c.total * pct }))
  }));
  const total = out.reduce((t, g) => t + g.total, 0);
  return { groups: out, total, myShare: total * pct };
}

app.get('/api/me/expense-summary', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  try {
    const companyId = parseInt(req.query.companyId, 10);
    const projectId = parseInt(req.query.projectId, 10);
    if (companyId) {
      const cp = (await pool.query(
        `SELECT cp.percentage, c.name FROM company_partners cp JOIN companies c ON c.id = cp.company_id
         WHERE cp.company_id=$1 AND cp.partner_id=$2`, [companyId, req.user.partnerId]
      )).rows[0];
      if (!cp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذه الشركة' });
      const rows = (await pool.query(`
        SELECT category, source, SUM(amount) AS total FROM (
          SELECT COALESCE(NULLIF(TRIM(e.category), ''), 'أخرى') AS category, 'project' AS source, e.amount
          FROM entries e JOIN projects p ON p.id = e.project_id
          WHERE e.kind = 'expense' AND p.company_id = $1
          UNION ALL
          SELECT COALESCE(NULLIF(TRIM(category), ''), 'أخرى'), 'company', amount
          FROM company_expenses WHERE company_id = $1
        ) t GROUP BY category, source
      `, [companyId])).rows;
      const byCat = {};
      let projectCostsTotal = 0, companyLevelTotal = 0;
      rows.forEach(r => {
        const t = Number(r.total);
        byCat[r.category] = (byCat[r.category] || 0) + t;
        if (r.source === 'project') projectCostsTotal += t; else companyLevelTotal += t;
      });
      const pct = Number(cp.percentage) / 100;
      const roll = await rollupForPartner(Object.keys(byCat).map(category => ({ category, total: byCat[category] })), pct);
      const collected = Number((await pool.query(`
        SELECT COALESCE(SUM(c.amount), 0) AS t FROM sale_collections c
        JOIN inventory_sales s ON s.id = c.sale_id JOIN projects p ON p.id = s.project_id WHERE p.company_id = $1
      `, [companyId])).rows[0].t);
      return res.json({ scope: 'company', name: cp.name, percentage: Number(cp.percentage),
        projectCostsTotal, companyLevelTotal, collected, myCollectedShare: collected * pct,
        settlement: collected * pct - roll.myShare, ...roll });
    }
    if (projectId) {
      const pp = (await pool.query(
        `SELECT pp.percentage, pr.name FROM project_partners pp JOIN projects pr ON pr.id = pp.project_id
         WHERE pp.project_id=$1 AND pp.partner_id=$2`, [projectId, req.user.partnerId]
      )).rows[0];
      if (!pp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذا المشروع' });
      const rows = (await pool.query(`
        SELECT COALESCE(NULLIF(TRIM(category), ''), 'أخرى') AS category, SUM(amount) AS total
        FROM entries WHERE kind = 'expense' AND project_id = $1 GROUP BY 1
      `, [projectId])).rows;
      const pct = Number(pp.percentage) / 100;
      const roll = await rollupForPartner(rows.map(r => ({ category: r.category, total: Number(r.total) })), pct);
      const collected = Number((await pool.query(`
        SELECT COALESCE(SUM(c.amount), 0) AS t FROM sale_collections c JOIN inventory_sales s ON s.id = c.sale_id WHERE s.project_id = $1
      `, [projectId])).rows[0].t);
      return res.json({ scope: 'project', name: pp.name, percentage: Number(pp.percentage),
        collected, myCollectedShare: collected * pct, settlement: collected * pct - roll.myShare, ...roll });
    }
    res.status(400).json({ error: 'companyId أو projectId مطلوب' });
  } catch (e) {
    console.error('partner expense summary failed', e);
    res.status(500).json({ error: 'تعذر تحميل ملخص المصاريف' });
  }
});

// Partner-facing period analysis of expenses (day / week / month / year): for every period the total, the partner's
// own share, and the same main-items breakdown as the summary (never line-level detail).
app.get('/api/me/expense-periods', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  try {
    const period = ['day', 'week', 'month', 'year'].includes(req.query.period) ? req.query.period : 'month';
    // the bucket expression only ever comes from this fixed list, never from the request
    const bucket = (col) => ({
      day: `to_char(${col}, 'YYYY-MM-DD')`,
      week: `to_char(date_trunc('week', ${col}::timestamp), 'YYYY-MM-DD')`,
      month: `to_char(${col}, 'YYYY-MM')`,
      year: `to_char(${col}, 'YYYY')`
    })[period];
    const companyId = parseInt(req.query.companyId, 10);
    const projectId = parseInt(req.query.projectId, 10);
    let name, pctNum, rows;
    if (companyId) {
      const cp = (await pool.query(
        `SELECT cp.percentage, c.name FROM company_partners cp JOIN companies c ON c.id = cp.company_id
         WHERE cp.company_id=$1 AND cp.partner_id=$2`, [companyId, req.user.partnerId]
      )).rows[0];
      if (!cp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذه الشركة' });
      name = cp.name; pctNum = Number(cp.percentage);
      rows = (await pool.query(`
        SELECT p, category, SUM(amount) AS total FROM (
          SELECT ${bucket('e.entry_date')} AS p, COALESCE(NULLIF(TRIM(e.category), ''), 'أخرى') AS category, e.amount
          FROM entries e JOIN projects pr ON pr.id = e.project_id WHERE e.kind = 'expense' AND pr.company_id = $1
          UNION ALL
          SELECT ${bucket('entry_date')}, COALESCE(NULLIF(TRIM(category), ''), 'أخرى'), amount FROM company_expenses WHERE company_id = $1
        ) t GROUP BY p, category
      `, [companyId])).rows;
    } else if (projectId) {
      const pp = (await pool.query(
        `SELECT pp.percentage, pr.name FROM project_partners pp JOIN projects pr ON pr.id = pp.project_id
         WHERE pp.project_id=$1 AND pp.partner_id=$2`, [projectId, req.user.partnerId]
      )).rows[0];
      if (!pp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذا المشروع' });
      name = pp.name; pctNum = Number(pp.percentage);
      rows = (await pool.query(`
        SELECT ${bucket('entry_date')} AS p, COALESCE(NULLIF(TRIM(category), ''), 'أخرى') AS category, SUM(amount) AS total
        FROM entries WHERE kind = 'expense' AND project_id = $1 GROUP BY 1, 2
      `, [projectId])).rows;
    } else {
      return res.status(400).json({ error: 'companyId أو projectId مطلوب' });
    }
    const pct = pctNum / 100, index = await loadExpenseIndex();
    const byPeriod = {};
    rows.forEach(r => { (byPeriod[r.p] = byPeriod[r.p] || []).push({ category: r.category, total: Number(r.total) }); });
    const periods = Object.keys(byPeriod).sort().reverse().map(key => {
      const roll = rollupWith(index, byPeriod[key], pct);
      return { key, total: roll.total, myShare: roll.myShare, groups: roll.groups };
    }).filter(x => Math.abs(x.total) >= 0.005);
    const total = periods.reduce((t, x) => t + x.total, 0);
    res.json({ scope: companyId ? 'company' : 'project', name, period, percentage: pctNum, total, myShare: total * pct, periods });
  } catch (e) {
    console.error('partner expense periods failed', e);
    res.status(500).json({ error: 'تعذر تحميل تحليل الفترات' });
  }
});

// Partner view of a company's fixed assets (read only), as ONE ROW PER CATEGORY: counts, cost, depreciation and book
// value with the partner's own share. Individual assets, their dates and notes never leave the server.
app.get('/api/me/assets', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  try {
    const companyId = parseInt(req.query.companyId, 10);
    if (!companyId) return res.status(400).json({ error: 'companyId مطلوب' });
    const cp = (await pool.query(
      `SELECT cp.percentage, c.name FROM company_partners cp JOIN companies c ON c.id = cp.company_id
       WHERE cp.company_id=$1 AND cp.partner_id=$2`, [companyId, req.user.partnerId]
    )).rows[0];
    if (!cp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذه الشركة' });
    const assets = (await pool.query(
      `SELECT category, purchase_date::text AS purchase_date, cost, useful_life_years FROM company_assets WHERE company_id=$1`, [companyId]
    )).rows.map(withAssetDepreciation);
    const pct = Number(cp.percentage) / 100;
    const groups = {};
    assets.forEach(a => {
      const k = (a.category || '').trim() || 'بدون تصنيف';
      const g = groups[k] || (groups[k] = { category: k, count: 0, cost: 0, accumulatedDepreciation: 0, bookValue: 0 });
      g.count++; g.cost += a.cost; g.accumulatedDepreciation += a.accumulatedDepreciation; g.bookValue += a.bookValue;
    });
    const categories = Object.values(groups).sort((x, y) => x.category.localeCompare(y.category, 'ar'))
      .map(g => ({ ...g, myCostShare: g.cost * pct, myBookShare: g.bookValue * pct }));
    const sum = (f) => categories.reduce((t, g) => t + f(g), 0);
    const cost = sum(g => g.cost), book = sum(g => g.bookValue);
    res.json({ companyId, name: cp.name, percentage: Number(cp.percentage), categories,
      totals: { count: sum(g => g.count), cost, accumulatedDepreciation: sum(g => g.accumulatedDepreciation), bookValue: book,
                myCostShare: cost * pct, myBookShare: book * pct } });
  } catch (e) {
    console.error('partner assets failed', e);
    res.status(500).json({ error: 'تعذر تحميل الأصول' });
  }
});

// Partner-facing MONTHLY expense analysis for one year: main items (the chart's groups and their direct items) as rows,
// the 12 months as columns, plus the partner's own share. Never line-level detail.
app.get('/api/me/expense-monthly', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  try {
    const companyId = parseInt(req.query.companyId, 10), projectId = parseInt(req.query.projectId, 10);
    const wantYear = parseInt(req.query.year, 10) || null;
    let name, pctNum, rows;
    if (companyId) {
      const cp = (await pool.query(
        `SELECT cp.percentage, c.name FROM company_partners cp JOIN companies c ON c.id = cp.company_id
         WHERE cp.company_id=$1 AND cp.partner_id=$2`, [companyId, req.user.partnerId]
      )).rows[0];
      if (!cp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذه الشركة' });
      name = cp.name; pctNum = Number(cp.percentage);
      rows = (await pool.query(`
        SELECT y, m, category, SUM(amount) AS total FROM (
          SELECT EXTRACT(YEAR FROM e.entry_date)::int AS y, EXTRACT(MONTH FROM e.entry_date)::int AS m,
                 COALESCE(NULLIF(TRIM(e.category), ''), 'أخرى') AS category, e.amount
          FROM entries e JOIN projects pr ON pr.id = e.project_id WHERE e.kind = 'expense' AND pr.company_id = $1
          UNION ALL
          SELECT EXTRACT(YEAR FROM entry_date)::int, EXTRACT(MONTH FROM entry_date)::int,
                 COALESCE(NULLIF(TRIM(category), ''), 'أخرى'), amount FROM company_expenses WHERE company_id = $1
        ) t GROUP BY y, m, category
      `, [companyId])).rows;
    } else if (projectId) {
      const pp = (await pool.query(
        `SELECT pp.percentage, pr.name FROM project_partners pp JOIN projects pr ON pr.id = pp.project_id
         WHERE pp.project_id=$1 AND pp.partner_id=$2`, [projectId, req.user.partnerId]
      )).rows[0];
      if (!pp) return res.status(403).json({ error: 'لا تملك صلاحية الوصول لهذا المشروع' });
      name = pp.name; pctNum = Number(pp.percentage);
      rows = (await pool.query(`
        SELECT EXTRACT(YEAR FROM entry_date)::int AS y, EXTRACT(MONTH FROM entry_date)::int AS m,
               COALESCE(NULLIF(TRIM(category), ''), 'أخرى') AS category, SUM(amount) AS total
        FROM entries WHERE kind = 'expense' AND project_id = $1 GROUP BY 1, 2, 3
      `, [projectId])).rows;
    } else {
      return res.status(400).json({ error: 'companyId أو projectId مطلوب' });
    }
    const years = [...new Set(rows.map(r => r.y))].sort((a, b) => b - a);
    const year = years.includes(wantYear) ? wantYear : (years[0] || new Date().getFullYear());
    const { byId, byNorm } = await loadExpenseIndex();
    const pathOf = (node) => { const path = []; let cur = node, guard = 0; while (cur && guard++ < 50) { path.unshift(cur); cur = cur.parent_id == null ? null : byId[cur.parent_id]; } return path; };
    const zeros = () => new Array(12).fill(0);
    const groups = {};
    rows.filter(r => r.y === year).forEach(r => {
      const t = Number(r.total), mi = r.m - 1;
      const node = byNorm[normCat(r.category)], path = node ? pathOf(node) : [];
      const root = path[0] || null, child = path[1] || null;
      const g = groups[root ? 'r' + root.id : 'x'] || (groups[root ? 'r' + root.id : 'x'] =
        { name: root ? root.name : UNCLASSIFIED_LABEL, rank: root ? root.rank : 1e9, months: zeros(), direct: zeros(), children: {} });
      g.months[mi] += t;
      if (child) { const c = g.children[child.id] || (g.children[child.id] = { name: child.name, rank: child.rank, months: zeros() }); c.months[mi] += t; }
      else g.direct[mi] += t;
    });
    const sum = (arr) => arr.reduce((a, b) => a + b, 0), nz = (arr) => arr.some(v => Math.abs(v) >= 0.005);
    const out = Object.values(groups).sort((a, b) => a.rank - b.rank).filter(g => nz(g.months)).map(g => ({
      name: g.name, months: g.months, total: sum(g.months),
      direct: nz(g.direct) ? { months: g.direct, total: sum(g.direct) } : null,
      children: Object.values(g.children).sort((a, b) => a.rank - b.rank).filter(c => nz(c.months)).map(c => ({ name: c.name, months: c.months, total: sum(c.months) }))
    }));
    const monthTotals = zeros(); out.forEach(g => g.months.forEach((v, i) => { monthTotals[i] += v; }));
    const pct = pctNum / 100, total = sum(monthTotals);
    res.json({ scope: companyId ? 'company' : 'project', name, percentage: pctNum, year, years, groups: out,
      monthTotals, total, myMonths: monthTotals.map(v => v * pct), myTotal: total * pct });
  } catch (e) {
    console.error('partner monthly expenses failed', e);
    res.status(500).json({ error: 'تعذر تحميل التحليل الشهري' });
  }
});

app.get('/api/report/:projectId', auth, async (req, res) => {
  const projectId = req.params.projectId;
  if (!(await assertProjectAccess(req, res, projectId))) return;

  const totalsRes = await pool.query(`
    SELECT
      COALESCE(SUM(CASE WHEN kind='revenue' THEN amount ELSE 0 END),0) AS revenue,
      COALESCE(SUM(CASE WHEN kind='expense' THEN amount ELSE 0 END),0) AS expense
    FROM entries WHERE project_id=$1
  `, [projectId]);
  const revenue = Number(totalsRes.rows[0].revenue);
  const expense = Number(totalsRes.rows[0].expense);
  const net = revenue - expense;

  const sharesRes = await pool.query(`
    SELECT pp.partner_id, pp.percentage, p.name
    FROM project_partners pp JOIN partners p ON p.id = pp.partner_id
    WHERE pp.project_id = $1
  `, [projectId]);

  const collected = Number((await pool.query(`
    SELECT COALESCE(SUM(c.amount), 0) AS t FROM sale_collections c JOIN inventory_sales s ON s.id = c.sale_id WHERE s.project_id = $1
  `, [projectId])).rows[0].t);

  const partners = sharesRes.rows.map(s => ({
    partnerId: s.partner_id,
    name: s.name,
    percentage: Number(s.percentage),
    shareOfNet: net * (Number(s.percentage) / 100),
    shareOfCollected: collected * (Number(s.percentage) / 100),
    shareOfExpenses: expense * (Number(s.percentage) / 100),
    settlement: (collected - expense) * (Number(s.percentage) / 100)
  }));

  res.json({ projectId: Number(projectId), revenue, expense, net, collected, collectedMinusExpenses: collected - expense, partners });
});

// ---------------------------------------------------------------------------
// Expense items (chart of expense categories): main items + nested sub-items
// ---------------------------------------------------------------------------
async function getExpenseTree() {
  const { rows } = await pool.query('SELECT id, name, parent_id FROM expense_items ORDER BY sort_order, id');
  const byParent = {};
  rows.forEach(r => {
    const k = r.parent_id == null ? 'root' : String(r.parent_id);
    (byParent[k] = byParent[k] || []).push(r);
  });
  const build = (pid) => (byParent[pid == null ? 'root' : String(pid)] || []).map(r => {
    const node = { id: r.id, n: r.name };
    const kids = build(r.id);
    if (kids.length) node.c = kids;
    return node;
  });
  return build(null);
}

function cleanItemName(v) {
  return String(v == null ? '' : v).replace(/\s+/g, ' ').trim();
}

app.get('/api/expense-items', auth, requireAdmin, async (req, res) => {
  res.json({ tree: await getExpenseTree() });
});

app.post('/api/expense-items', auth, requireAdmin, async (req, res) => {
  const name = cleanItemName((req.body || {}).name);
  const parentId = (req.body || {}).parentId == null ? null : parseInt(req.body.parentId, 10);
  if (!name) return res.status(400).json({ error: 'اسم البند مطلوب' });
  if (name.length > 200) return res.status(400).json({ error: 'اسم البند طويل جدًا' });
  if ((req.body || {}).parentId != null && !parentId) return res.status(400).json({ error: 'البند الرئيسي غير صالح' });
  try {
    if (parentId != null) {
      const p = await pool.query('SELECT 1 FROM expense_items WHERE id=$1', [parentId]);
      if (!p.rows.length) return res.status(404).json({ error: 'البند الرئيسي غير موجود' });
    }
    const all = (await pool.query('SELECT name FROM expense_items')).rows;
    if (all.some(r => normCat(r.name) === normCat(name))) {
      return res.status(409).json({ error: 'يوجد بند بنفس الاسم بالفعل' });
    }
    const order = (await pool.query(
      'SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM expense_items WHERE parent_id IS NOT DISTINCT FROM $1::int',
      [parentId]
    )).rows[0].n;
    const { rows } = await pool.query(
      'INSERT INTO expense_items (name, parent_id, sort_order) VALUES ($1,$2,$3) RETURNING id, name, parent_id',
      [name, parentId, order]
    );
    res.json(rows[0]);
  } catch (e) {
    console.error('add expense item failed', e);
    res.status(500).json({ error: 'تعذر إضافة البند: ' + e.message });
  }
});

// Rename an item; already-recorded expenses that used the old name follow the new name
app.put('/api/expense-items/:id', auth, requireAdmin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  const name = cleanItemName((req.body || {}).name);
  if (!id) return res.status(400).json({ error: 'بند غير صالح' });
  if (!name) return res.status(400).json({ error: 'اسم البند مطلوب' });
  if (name.length > 200) return res.status(400).json({ error: 'اسم البند طويل جدًا' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const cur = (await client.query('SELECT name FROM expense_items WHERE id=$1', [id])).rows[0];
    if (!cur) { await client.query('ROLLBACK'); return res.status(404).json({ error: 'البند غير موجود' }); }
    const others = (await client.query('SELECT name FROM expense_items WHERE id <> $1', [id])).rows;
    if (others.some(r => normCat(r.name) === normCat(name))) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'يوجد بند بنفس الاسم بالفعل' });
    }
    await client.query('UPDATE expense_items SET name=$1 WHERE id=$2', [name, id]);
    const cats = (await client.query(`
      SELECT category FROM entries WHERE kind='expense' AND category IS NOT NULL
      UNION SELECT category FROM company_expenses WHERE category IS NOT NULL
    `)).rows.map(r => r.category).filter(c => normCat(c) === normCat(cur.name));
    let updated = 0;
    if (cats.length) {
      updated += (await client.query(
        `UPDATE entries SET category=$1 WHERE kind='expense' AND category = ANY($2::text[])`, [name, cats]
      )).rowCount;
      updated += (await client.query(
        `UPDATE company_expenses SET category=$1 WHERE category = ANY($2::text[])`, [name, cats]
      )).rowCount;
    }
    await client.query('COMMIT');
    res.json({ ok: true, updatedRecords: updated });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('rename expense item failed', e);
    res.status(500).json({ error: 'تعذر تعديل البند: ' + e.message });
  } finally {
    client.release();
  }
});

// Delete an item. Items with sub-items need ?cascade=1. Recorded expenses are kept (they show as unclassified).
app.delete('/api/expense-items/:id', auth, requireAdmin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!id) return res.status(400).json({ error: 'بند غير صالح' });
  const cur = await pool.query('SELECT 1 FROM expense_items WHERE id=$1', [id]);
  if (!cur.rows.length) return res.status(404).json({ error: 'البند غير موجود' });
  const kids = (await pool.query('SELECT COUNT(*)::int AS c FROM expense_items WHERE parent_id=$1', [id])).rows[0].c;
  if (kids > 0 && req.query.cascade !== '1') {
    return res.status(409).json({ error: 'هذا البند له بنود فرعية. احذفها أولًا أو أكّد حذف الكل.', hasChildren: true });
  }
  await pool.query('DELETE FROM expense_items WHERE id=$1', [id]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Expense ledger (كشف حساب المصاريف): مدين / دائن / رصيد آخر كل شهر، لنفس النطاق
// والسنة المستخدمين في تحليل المصاريف الشهري، فيتطابق الصافي الشهري بين الاثنين.
// ---------------------------------------------------------------------------
app.get('/api/reports/expense-ledger', auth, requireAdmin, async (req, res) => {
  try {
    const scope = String(req.query.scope || 'all');
    const params0 = [];
    let entriesFilter = '';
    let companyFilter = '';
    if (scope.startsWith('company:')) {
      const id = parseInt(scope.slice(8), 10);
      if (!id) return res.status(400).json({ error: 'نطاق غير صالح' });
      params0.push(id);
      entriesFilter = ' AND e.project_id IN (SELECT id FROM projects WHERE company_id = $1)';
      companyFilter = ' AND ce.company_id = $1';
    } else if (scope.startsWith('project:')) {
      const id = parseInt(scope.slice(8), 10);
      if (!id) return res.status(400).json({ error: 'نطاق غير صالح' });
      params0.push(id);
      entriesFilter = ' AND e.project_id = $1';
      companyFilter = ' AND FALSE';
    } else if (scope !== 'all') {
      return res.status(400).json({ error: 'نطاق غير صالح' });
    }

    const yearRows = (await pool.query(`
      SELECT DISTINCT y FROM (
        SELECT EXTRACT(YEAR FROM e.entry_date)::int AS y FROM entries e WHERE e.kind='expense' ${entriesFilter}
        UNION
        SELECT EXTRACT(YEAR FROM ce.entry_date)::int AS y FROM company_expenses ce WHERE TRUE ${companyFilter}
      ) t ORDER BY y
    `, params0)).rows;
    const years = yearRows.map(r => r.y);
    let year = parseInt(req.query.year, 10);
    if (!year) year = years.length ? years[years.length - 1] : new Date().getFullYear();

    const priorRow = (await pool.query(`
      SELECT
        COALESCE((SELECT SUM(e.amount) FROM entries e WHERE e.kind='expense' AND e.entry_date < ($${params0.length+1} || '-01-01')::date ${entriesFilter}), 0)
        + COALESCE((SELECT SUM(ce.amount) FROM company_expenses ce WHERE ce.entry_date < ($${params0.length+1} || '-01-01')::date ${companyFilter}), 0) AS t
    `, [...params0, year])).rows[0];
    const openingBalanceYear = Number(priorRow.t);

    const monthRows = (await pool.query(`
      SELECT m, SUM(debit) AS debit, SUM(credit) AS credit FROM (
        SELECT EXTRACT(MONTH FROM e.entry_date)::int AS m,
          GREATEST(e.amount,0) AS debit, GREATEST(-e.amount,0) AS credit
        FROM entries e WHERE e.kind='expense' AND EXTRACT(YEAR FROM e.entry_date)::int = $${params0.length+1} ${entriesFilter}
        UNION ALL
        SELECT EXTRACT(MONTH FROM ce.entry_date)::int AS m,
          GREATEST(ce.amount,0) AS debit, GREATEST(-ce.amount,0) AS credit
        FROM company_expenses ce WHERE EXTRACT(YEAR FROM ce.entry_date)::int = $${params0.length+1} ${companyFilter}
      ) t GROUP BY m
    `, [...params0, year])).rows;
    const byMonth = {};
    monthRows.forEach(r => { byMonth[r.m] = { debit: Number(r.debit), credit: Number(r.credit) }; });

    let running = openingBalanceYear;
    const months = [];
    for (let m = 1; m <= 12; m++) {
      const d = byMonth[m] || { debit: 0, credit: 0 };
      running += d.debit - d.credit;
      months.push({ month: m, debit: d.debit, credit: d.credit, balance: running });
    }

    res.json({ scope, year, years, openingBalanceYear, closingBalanceYear: running, months });
  } catch (e) {
    console.error('expense ledger failed', e);
    res.status(500).json({ error: 'تعذر تحميل كشف حساب المصاريف: ' + e.message });
  }
});

// ---------------------------------------------------------------------------
// Expense analysis: monthly totals per expense item (category), admin only.
// Combines project expenses (entries) and company-level expenses.
// scope = all | company:<id> | project:<id>
// ---------------------------------------------------------------------------
app.get('/api/reports/expense-analysis', auth, requireAdmin, async (req, res) => {
  try {
    const scope = String(req.query.scope || 'all');

    const yearRows = (await pool.query(`
      SELECT DISTINCT y FROM (
        SELECT EXTRACT(YEAR FROM entry_date)::int AS y FROM entries WHERE kind='expense'
        UNION
        SELECT EXTRACT(YEAR FROM entry_date)::int AS y FROM company_expenses
      ) t ORDER BY y DESC
    `)).rows;
    const years = yearRows.map(r => r.y);

    let year = parseInt(req.query.year, 10);
    if (!year) year = years.length ? years[0] : new Date().getFullYear();

    const params = [year];
    let entriesFilter = '';
    let companyFilter = '';
    if (scope.startsWith('company:')) {
      const id = parseInt(scope.slice(8), 10);
      if (!id) return res.status(400).json({ error: 'نطاق غير صالح' });
      params.push(id);
      entriesFilter = ' AND e.project_id IN (SELECT id FROM projects WHERE company_id = $2)';
      companyFilter = ' AND ce.company_id = $2';
    } else if (scope.startsWith('project:')) {
      const id = parseInt(scope.slice(8), 10);
      if (!id) return res.status(400).json({ error: 'نطاق غير صالح' });
      params.push(id);
      entriesFilter = ' AND e.project_id = $2';
      companyFilter = ' AND FALSE'; // a single project has no company-level expenses
    } else if (scope !== 'all') {
      return res.status(400).json({ error: 'نطاق غير صالح' });
    }

    const { rows } = await pool.query(`
      SELECT category, m, SUM(total) AS total FROM (
        SELECT COALESCE(NULLIF(TRIM(e.category), ''), 'أخرى') AS category,
               EXTRACT(MONTH FROM e.entry_date)::int AS m, SUM(e.amount) AS total
        FROM entries e
        WHERE e.kind = 'expense' AND EXTRACT(YEAR FROM e.entry_date)::int = $1::int ${entriesFilter}
        GROUP BY 1, 2
        UNION ALL
        SELECT COALESCE(NULLIF(TRIM(ce.category), ''), 'أخرى') AS category,
               EXTRACT(MONTH FROM ce.entry_date)::int AS m, SUM(ce.amount) AS total
        FROM company_expenses ce
        WHERE EXTRACT(YEAR FROM ce.entry_date)::int = $1::int ${companyFilter}
        GROUP BY 1, 2
      ) t
      GROUP BY category, m
      ORDER BY category, m
    `, params);

    const byCat = {};
    rows.forEach(r => {
      if (!byCat[r.category]) byCat[r.category] = new Array(12).fill(0);
      byCat[r.category][r.m - 1] = Number(r.total);
    });
    const items = Object.keys(byCat).map(category => ({ category, months: byCat[category] }));

    res.json({ year, scope, years, rows: items, chart: await getExpenseTree() });
  } catch (e) {
    console.error('expense analysis failed', e);
    res.status(500).json({ error: 'تعذر تحميل تحليل المصاريف: ' + e.message });
  }
});

// ---------------------------------------------------------------------------
// Bulk import from Excel (parsed client-side, sent here as JSON rows)
// ---------------------------------------------------------------------------
app.post('/api/entries/bulk', auth, requireAdmin, async (req, res) => {
  const { projectId, kind, rows } = req.body || {};
  if (!projectId || !['revenue', 'expense'].includes(kind) || !Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: 'بيانات ناقصة' });
  }
  const valid = rows.filter(r => r.amount && r.date && isValidISODate(r.date));
  if (!valid.length) return res.status(400).json({ error: 'لا يوجد صفوف صالحة للاستيراد (تحقق من صيغة التاريخ)' });
  const amounts = valid.map(r => r.amount);
  const descriptions = valid.map(r => r.description || null);
  const dates = valid.map(r => r.date);
  const categories = valid.map(r => (kind === 'expense' ? (r.category || 'أخرى') : null));
  try {
    await pool.query(
      `INSERT INTO entries (project_id, kind, amount, description, entry_date, category)
       SELECT $1, $2, u.amount, u.description, u.entry_date, u.category
       FROM UNNEST($3::numeric[], $4::text[], $5::date[], $6::text[]) AS u(amount, description, entry_date, category)`,
      [projectId, kind, amounts, descriptions, dates, categories]
    );
    res.json({ inserted: valid.length });
  } catch (e) {
    console.error('entries bulk import failed', e);
    res.status(500).json({ error: 'فشل الاستيراد: ' + e.message });
  }
});

app.post('/api/inventory/items/bulk', auth, requireAdmin, async (req, res) => {
  const { projectId, rows } = req.body || {};
  if (!projectId || !Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  const valid = rows.filter(r => r.name && r.quantityIn !== undefined && r.quantityIn !== null);
  if (!valid.length) return res.status(400).json({ error: 'لا يوجد صفوف صالحة للاستيراد' });
  try {
    await pool.query(
      `INSERT INTO inventory_items (project_id, name, unit, quantity_in, unit_price, status, net_area, garden_area, building)
       SELECT $1, u.name, u.unit, u.quantity_in, u.unit_price, u.status, u.net_area, u.garden_area, u.building
       FROM UNNEST($2::text[], $3::text[], $4::numeric[], $5::numeric[], $6::text[], $7::numeric[], $8::numeric[], $9::text[])
         AS u(name, unit, quantity_in, unit_price, status, net_area, garden_area, building)`,
      [projectId,
       valid.map(r => r.name), valid.map(r => r.unit || null),
       valid.map(r => r.quantityIn), valid.map(r => r.unitPrice || 0),
       valid.map(r => r.status || null), valid.map(r => r.netArea || null), valid.map(r => r.gardenArea || null),
       valid.map(r => r.building || null)]
    );
    res.json({ inserted: valid.length });
  } catch (e) {
    console.error('inventory items bulk import failed', e);
    res.status(500).json({ error: 'فشل الاستيراد: ' + e.message });
  }
});

app.post('/api/current-account/bulk', auth, requireAdmin, async (req, res) => {
  const { companyId, rows } = req.body || {}; // rows: [{partnerId, kind, amount, date, description}]
  if (!companyId || !Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  const valid = rows.filter(r => r.partnerId && r.amount && r.date && isValidISODate(r.date) && ['deposit', 'withdrawal', 'distribution'].includes(r.kind));
  if (!valid.length) return res.status(400).json({ error: 'لا يوجد صفوف صالحة للاستيراد (تحقق من صيغة التاريخ والنوع)' });
  try {
    await pool.query(
      `INSERT INTO current_account (company_id, partner_id, kind, amount, description, entry_date)
       SELECT $1, u.partner_id, u.kind, u.amount, u.description, u.entry_date
       FROM UNNEST($2::int[], $3::text[], $4::numeric[], $5::text[], $6::date[])
         AS u(partner_id, kind, amount, description, entry_date)`,
      [companyId, valid.map(r => r.partnerId), valid.map(r => r.kind),
       valid.map(r => r.amount), valid.map(r => r.description || null), valid.map(r => r.date)]
    );
    res.json({ inserted: valid.length });
  } catch (e) {
    console.error('current-account bulk import failed', e);
    res.status(500).json({ error: 'فشل الاستيراد: ' + e.message });
  }
});

// ---------------------------------------------------------------------------
// Static frontend
// ---------------------------------------------------------------------------
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
  })
  .catch(err => {
    console.error('Failed to initialize database', err);
    process.exit(1);
  });
