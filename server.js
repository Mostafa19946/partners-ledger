const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const path = require('path');

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'change-this-secret';
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin123';
const ADMIN_NAME = process.env.ADMIN_NAME || 'مدير الحسابات';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('render.com')
    ? { rejectUnauthorized: false }
    : false
});

const app = express();
app.use(cors());
app.use(express.json());

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
      project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
      partner_id INTEGER NOT NULL REFERENCES partners(id) ON DELETE CASCADE,
      kind TEXT NOT NULL CHECK (kind IN ('deposit','withdrawal','distribution')),
      amount NUMERIC NOT NULL,
      description TEXT,
      entry_date DATE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
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
    SELECT cp.company_id, cp.partner_id, cp.percentage, p.name AS partner_name
    FROM company_partners cp JOIN partners p ON p.id = cp.partner_id
  `)).rows;
  const withShares = companies.map(c => ({
    ...c,
    partners: shares.filter(s => s.company_id === c.id)
      .map(s => ({ partnerId: s.partner_id, percentage: Number(s.percentage), name: s.partner_name }))
  }));
  res.json(withShares);
});

app.post('/api/companies', auth, requireAdmin, async (req, res) => {
  const { name } = req.body || {};
  if (!name) return res.status(400).json({ error: 'اسم الشركة مطلوب' });
  const { rows } = await pool.query('INSERT INTO companies (name) VALUES ($1) RETURNING *', [name]);
  res.json(rows[0]);
});

app.delete('/api/companies/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('UPDATE projects SET company_id=NULL WHERE company_id=$1', [req.params.id]);
  await pool.query('DELETE FROM companies WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.post('/api/companies/:id/partners', auth, requireAdmin, async (req, res) => {
  const { partnerId, percentage } = req.body || {};
  if (!partnerId || percentage === undefined) return res.status(400).json({ error: 'بيانات ناقصة' });
  try {
    const { rows } = await pool.query(
      'INSERT INTO company_partners (company_id, partner_id, percentage) VALUES ($1,$2,$3) RETURNING *',
      [req.params.id, partnerId, percentage]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(409).json({ error: 'هذا الشريك مضاف بالفعل لهذه الشركة' });
  }
});

app.put('/api/companies/:id/partners/:partnerId', auth, requireAdmin, async (req, res) => {
  const { percentage } = req.body || {};
  const { rows } = await pool.query(
    'UPDATE company_partners SET percentage=$1 WHERE company_id=$2 AND partner_id=$3 RETURNING *',
    [percentage, req.params.id, req.params.partnerId]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

app.delete('/api/companies/:id/partners/:partnerId', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM company_partners WHERE company_id=$1 AND partner_id=$2', [req.params.id, req.params.partnerId]);
  res.json({ ok: true });
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

app.get('/api/entries', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(
    'SELECT * FROM entries WHERE project_id=$1 ORDER BY entry_date DESC, created_at DESC',
    [projectId]
  );
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

app.delete('/api/entries/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM entries WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
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
  res.json(rows);
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
  const { projectId, name, unit, quantityIn, unitPrice, status, netArea, gardenArea } = req.body || {};
  if (!projectId || !name || quantityIn === undefined) return res.status(400).json({ error: 'بيانات ناقصة' });
  const { rows } = await pool.query(
    'INSERT INTO inventory_items (project_id, name, unit, quantity_in, unit_price, status, net_area, garden_area) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',
    [projectId, name, unit || null, quantityIn, unitPrice || 0, status || null, netArea || null, gardenArea || null]
  );
  res.json(rows[0]);
});

app.delete('/api/inventory/items/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM inventory_items WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

app.get('/api/inventory/sales', auth, async (req, res) => {
  const projectId = req.query.projectId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const { rows } = await pool.query(`
    SELECT s.*, i.name AS item_name, i.unit AS item_unit, i.net_area AS item_net_area, i.garden_area AS item_garden_area
    FROM inventory_sales s JOIN inventory_items i ON i.id = s.item_id
    WHERE s.project_id=$1 ORDER BY s.sale_date DESC, s.created_at DESC
  `, [projectId]);
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
  res.json(rows[0]);
});

app.put('/api/inventory/sales/:id', auth, requireAdmin, async (req, res) => {
  const b = req.body || {};
  const v = (x) => (x === undefined ? null : x);
  const { rows } = await pool.query(
    `UPDATE inventory_sales SET
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
     v(b.downPaymentPercent), req.params.id]
  );
  if (!rows.length) return res.status(404).json({ error: 'غير موجود' });
  res.json(rows[0]);
});

app.delete('/api/inventory/sales/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM inventory_sales WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
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
  const { rows } = await pool.query(
    'INSERT INTO sale_collections (sale_id, amount, collection_date, description) VALUES ($1,$2,$3,$4) RETURNING *',
    [saleId, amount, date, description || null]
  );
  res.json(rows[0]);
});

app.delete('/api/inventory/collections/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM sale_collections WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Current account
// ---------------------------------------------------------------------------
app.get('/api/current-account', auth, async (req, res) => {
  const projectId = req.query.projectId;
  const partnerId = req.query.partnerId;
  if (!projectId) return res.status(400).json({ error: 'projectId مطلوب' });
  if (!(await assertProjectAccess(req, res, projectId))) return;
  const effectivePartnerId = req.user.role === 'admin' ? partnerId : req.user.partnerId;
  let query = 'SELECT * FROM current_account WHERE project_id=$1';
  const params = [projectId];
  if (effectivePartnerId) {
    params.push(effectivePartnerId);
    query += ` AND partner_id=$${params.length}`;
  }
  query += ' ORDER BY entry_date DESC, created_at DESC';
  const { rows } = await pool.query(query, params);
  res.json(rows);
});

app.post('/api/current-account', auth, requireAdmin, async (req, res) => {
  const { projectId, partnerId, kind, amount, description, date } = req.body || {};
  if (!projectId || !partnerId || !['deposit', 'withdrawal', 'distribution'].includes(kind) || !amount || !date) {
    return res.status(400).json({ error: 'بيانات ناقصة' });
  }
  const { rows } = await pool.query(
    'INSERT INTO current_account (project_id, partner_id, kind, amount, description, entry_date) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
    [projectId, partnerId, kind, amount, description || null, date]
  );
  res.json(rows[0]);
});

app.delete('/api/current-account/:id', auth, requireAdmin, async (req, res) => {
  await pool.query('DELETE FROM current_account WHERE id=$1', [req.params.id]);
  res.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Partner: my projects + summary
// ---------------------------------------------------------------------------
app.get('/api/me/projects', auth, async (req, res) => {
  if (req.user.role !== 'partner') return res.status(403).json({ error: 'غير متاح' });
  const { rows } = await pool.query(`
    SELECT pr.id, pr.name, pp.percentage
    FROM projects pr
    JOIN project_partners pp ON pp.project_id = pr.id
    WHERE pp.partner_id = $1
    ORDER BY pr.id
  `, [req.user.partnerId]);
  res.json(rows);
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
    SELECT pp.partner_id, pp.percentage, pp.opening_balance, p.name
    FROM project_partners pp JOIN partners p ON p.id = pp.partner_id
    WHERE pp.project_id = $1
  `, [projectId]);

  const balancesRes = await pool.query(`
    SELECT partner_id,
      COALESCE(SUM(CASE WHEN kind IN ('deposit','distribution') THEN amount ELSE -amount END),0) AS balance
    FROM current_account WHERE project_id=$1
    GROUP BY partner_id
  `, [projectId]);
  const balanceMap = {};
  balancesRes.rows.forEach(r => { balanceMap[r.partner_id] = Number(r.balance); });

  const partners = sharesRes.rows.map(s => ({
    partnerId: s.partner_id,
    name: s.name,
    percentage: Number(s.percentage),
    openingBalance: Number(s.opening_balance),
    shareOfNet: net * (Number(s.percentage) / 100),
    currentAccountBalance: (balanceMap[s.partner_id] || 0) + Number(s.opening_balance)
  }));

  res.json({ projectId: Number(projectId), revenue, expense, net, partners });
});

// ---------------------------------------------------------------------------
// Bulk import from Excel (parsed client-side, sent here as JSON rows)
// ---------------------------------------------------------------------------
app.post('/api/entries/bulk', auth, requireAdmin, async (req, res) => {
  const { projectId, kind, rows } = req.body || {};
  if (!projectId || !['revenue', 'expense'].includes(kind) || !Array.isArray(rows) || !rows.length) {
    return res.status(400).json({ error: 'بيانات ناقصة' });
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let inserted = 0;
    for (const r of rows) {
      if (!r.amount || !r.date) continue;
      await client.query(
        'INSERT INTO entries (project_id, kind, amount, description, entry_date, category) VALUES ($1,$2,$3,$4,$5,$6)',
        [projectId, kind, r.amount, r.description || null, r.date, kind === 'expense' ? (r.category || 'أخرى') : null]
      );
      inserted++;
    }
    await client.query('COMMIT');
    res.json({ inserted });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'فشل الاستيراد' });
  } finally {
    client.release();
  }
});

app.post('/api/inventory/items/bulk', auth, requireAdmin, async (req, res) => {
  const { projectId, rows } = req.body || {};
  if (!projectId || !Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let inserted = 0;
    for (const r of rows) {
      if (!r.name || r.quantityIn === undefined || r.quantityIn === null) continue;
      await client.query(
        'INSERT INTO inventory_items (project_id, name, unit, quantity_in, unit_price, status, net_area, garden_area) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
        [projectId, r.name, r.unit || null, r.quantityIn, r.unitPrice || 0, r.status || null, r.netArea || null, r.gardenArea || null]
      );
      inserted++;
    }
    await client.query('COMMIT');
    res.json({ inserted });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'فشل الاستيراد' });
  } finally {
    client.release();
  }
});

app.post('/api/current-account/bulk', auth, requireAdmin, async (req, res) => {
  const { projectId, rows } = req.body || {}; // rows: [{partnerId, kind, amount, date, description}]
  if (!projectId || !Array.isArray(rows) || !rows.length) return res.status(400).json({ error: 'بيانات ناقصة' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let inserted = 0;
    for (const r of rows) {
      if (!r.partnerId || !r.amount || !r.date || !['deposit', 'withdrawal', 'distribution'].includes(r.kind)) continue;
      await client.query(
        'INSERT INTO current_account (project_id, partner_id, kind, amount, description, entry_date) VALUES ($1,$2,$3,$4,$5,$6)',
        [projectId, r.partnerId, r.kind, r.amount, r.description || null, r.date]
      );
      inserted++;
    }
    await client.query('COMMIT');
    res.json({ inserted });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'فشل الاستيراد' });
  } finally {
    client.release();
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
