-- Warehouse Price Platform — PostgreSQL schema (v1, rebuilt)
-- Use UUIDs if preferred; SERIAL used here for simplicity.

CREATE TABLE supplier (
  id                SERIAL PRIMARY KEY,
  name              TEXT NOT NULL UNIQUE,
  account_code      TEXT,
  supports_eti      BOOLEAN DEFAULT FALSE,
  terms             TEXT
);

CREATE TABLE product (
  id                SERIAL PRIMARY KEY,
  medicare_pip      TEXT NOT NULL UNIQUE,
  name              TEXT NOT NULL,
  pack_size         TEXT,
  is_brand          BOOLEAN DEFAULT FALSE,
  status            TEXT DEFAULT 'active'
);

CREATE TABLE dmd_item (
  id                SERIAL PRIMARY KEY,
  vmpp_id           TEXT,
  ampp_id           TEXT,
  vtm_id            TEXT,
  dt_cat            TEXT,
  dt_price          NUMERIC(12,4),
  dt_pack_size      TEXT,
  effective_date    DATE NOT NULL
);

CREATE TABLE product_mapping (
  id                SERIAL PRIMARY KEY,
  product_id        INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
  dmd_item_id       INTEGER NOT NULL REFERENCES dmd_item(id) ON DELETE RESTRICT,
  confidence_score  NUMERIC(5,2),
  approved_by       TEXT,
  approved_at       TIMESTAMP WITH TIME ZONE
);

CREATE TABLE supplier_item (
  id                SERIAL PRIMARY KEY,
  supplier_id       INTEGER NOT NULL REFERENCES supplier(id) ON DELETE CASCADE,
  product_id        INTEGER NOT NULL REFERENCES product(id) ON DELETE CASCADE,
  supplier_sku      TEXT,
  barcode           TEXT,
  pack_size_equiv   TEXT,
  active            BOOLEAN DEFAULT TRUE,
  UNIQUE (supplier_id, product_id)
);

CREATE TABLE price_quote (
  id                SERIAL PRIMARY KEY,
  supplier_item_id  INTEGER REFERENCES supplier_item(id) ON DELETE SET NULL,
  supplier_id       INTEGER REFERENCES supplier(id) ON DELETE SET NULL,
  product_id        INTEGER REFERENCES product(id) ON DELETE SET NULL,
  channel           TEXT, -- Direct / Proposition / T&R / Short-dated / Spot / Promo / Tender / (empty)
  price_ex_vat      NUMERIC(12,4) NOT NULL,
  discount          NUMERIC(8,4),
  valid_from        DATE NOT NULL,
  quoted_on         DATE NOT NULL,
  batch_id          TEXT NOT NULL,
  source_column     TEXT,
  CHECK (price_ex_vat > 0)
);

CREATE INDEX idx_price_quote_pf ON price_quote (product_id, valid_from);
CREATE INDEX idx_price_quote_sf ON price_quote (supplier_id, valid_from);

CREATE TABLE price_rule (
  id                SERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  scope             TEXT NOT NULL, -- global / supplier / product / category
  formula_json      JSONB NOT NULL, -- e.g. {"type":"clawback","rate":0.085}
  valid_from        DATE NOT NULL,
  valid_to          DATE
);

CREATE TABLE computed_price (
  product_id         INTEGER PRIMARY KEY REFERENCES product(id) ON DELETE CASCADE,
  best_supplier_id   INTEGER REFERENCES supplier(id),
  best_unit_cost     NUMERIC(12,4),
  margin_after_rules NUMERIC(8,4),
  computed_on        TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE purchase_order (
  id                SERIAL PRIMARY KEY,
  supplier_id       INTEGER NOT NULL REFERENCES supplier(id) ON DELETE RESTRICT,
  created_by        TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'draft',
  exported_at       TIMESTAMP WITH TIME ZONE
);

CREATE TABLE po_line (
  id                SERIAL PRIMARY KEY,
  po_id             INTEGER NOT NULL REFERENCES purchase_order(id) ON DELETE CASCADE,
  product_id        INTEGER NOT NULL REFERENCES product(id) ON DELETE RESTRICT,
  supplier_item_id  INTEGER REFERENCES supplier_item(id) ON DELETE SET NULL,
  qty               INTEGER NOT NULL CHECK (qty > 0),
  unit_price        NUMERIC(12,4) NOT NULL,
  discount          NUMERIC(8,4),
  price_quote_id    INTEGER REFERENCES price_quote(id) ON DELETE SET NULL
);

CREATE TABLE import_log (
  id                SERIAL PRIMARY KEY,
  file_name         TEXT NOT NULL,
  file_hash         TEXT NOT NULL,
  import_type       TEXT NOT NULL, -- supplier_price, dmd
  rows_in           INTEGER,
  rows_loaded       INTEGER,
  errors            JSONB,
  batch_id          TEXT NOT NULL,
  created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE audit_log (
  id                SERIAL PRIMARY KEY,
  user_name         TEXT,
  action            TEXT,
  entity_type       TEXT,
  entity_id         TEXT,
  before_json       JSONB,
  after_json        JSONB,
  occurred_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE reference_column (
  id                SERIAL PRIMARY KEY,
  column_name       TEXT NOT NULL,
  notes             TEXT,
  last_seen_on      DATE
);
