/**
 * Airtable-PostgreSQL Adapter
 * Version: 1.0
 * Date: January 28, 2026
 * 
 * This adapter mimics the Airtable SDK interface so existing Lambda code
 * works with PostgreSQL without modifications.
 * 
 * Usage:
 *   // Before (Airtable):
 *   const Airtable = require('airtable');
 *   const base = new Airtable({ apiKey: 'xxx' }).base('appXXX');
 *   
 *   // After (PostgreSQL):
 *   const { AirtablePostgresAdapter } = require('./airtable-postgres-adapter');
 *   const base = new AirtablePostgresAdapter(pgConfig, 'haq_scoring');
 * 
 * Supported methods:
 *   - base('tableName').select({ filterByFormula, maxRecords, sort }).firstPage()
 *   - base('tableName').select({ filterByFormula }).all()
 *   - base('tableName').find(recordId)
 *   - base('tableName').update(recordId, fields)
 *   - base('tableName').create(fields)
 *   - base('tableName').destroy(recordIds)
 *   - record.fields, record.id, record.get('fieldName')
 */

const { Client, Pool } = require('pg');

// Schema configuration - configurable via env var, no code changes needed if schema moves
const DB_SCHEMA_FILTER = (process.env.DB_SCHEMAS || 'scoring,public')
  .split(',').map(s => `'${s.trim()}'`).join(', ');

// ═══════════════════════════════════════════════════════════════════════════════
// AIRTABLE RECORD CLASS
// Mimics Airtable record structure with .fields, .id, and .get() method
// ═══════════════════════════════════════════════════════════════════════════════

class AirtableRecord {
  constructor(row, fieldMapping) {
    this.id = row.airtable_record_id || row.id?.toString();
    this.createdTime = row.airtable_created_time;
    this._row = row;
    this._fieldMapping = fieldMapping;
    
    // Build fields object with original Airtable field names
    this.fields = {};
    for (const [pgCol, airtableField] of Object.entries(fieldMapping)) {
      if (row[pgCol] !== undefined && pgCol !== 'id' && pgCol !== 'airtable_record_id' && pgCol !== 'airtable_created_time') {
        // Parse JSON strings back to arrays/objects
        let value = row[pgCol];
        if (typeof value === 'string' && (value.startsWith('[') || value.startsWith('{'))) {
          try {
            value = JSON.parse(value);
          } catch (e) {
            // Keep as string if not valid JSON
          }
        }
        this.fields[airtableField] = value;
      }
    }
  }
  
  /**
   * Get field value (mimics Airtable SDK .get() method)
   * Handles array unwrapping for linked records
   */
  get(fieldName) {
    const value = this.fields[fieldName];
    // Don't auto-unwrap arrays - return as-is like Airtable does
    return value;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// QUERY BUILDER CLASS
// Mimics Airtable's fluent query interface
// ═══════════════════════════════════════════════════════════════════════════════

class AirtableQuery {
  constructor(adapter, tableName) {
    this.adapter = adapter;
    this.tableName = tableName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
    this.options = {};
  }
  
  select(options = {}) {
    this.options = options;
    return this;
  }
  
  async firstPage() {
    const records = await this._executeQuery(this.options.maxRecords || 100);
    return records;
  }
  
  async all() {
    const records = await this._executeQuery(10000); // Large limit for .all()
    return records;
  }
  
  async _executeQuery(limit) {
    const client = await this.adapter.pool.connect();
    
    try {
      // Get column mapping for this table
      const fieldMapping = await this._getFieldMapping(client);
      
      // Build SQL query
      let sql = `SELECT * FROM "${this.tableName}"`;
      const params = [];
      
      // Parse filterByFormula (now async to support linked record resolution)
      if (this.options.filterByFormula) {
        const whereClause = await this._parseFilterFormula(this.options.filterByFormula, fieldMapping, client);
        if (whereClause) {
          sql += ` WHERE ${whereClause.sql}`;
          params.push(...whereClause.params);
        }
      }
      
      // Add sorting
      if (this.options.sort && this.options.sort.length > 0) {
        const sortClauses = this.options.sort.map(s => {
          const col = this._findColumn(s.field, fieldMapping);
          return `"${col}" ${s.direction === 'desc' ? 'DESC' : 'ASC'}`;
        });
        sql += ` ORDER BY ${sortClauses.join(', ')}`;
      }
      
      // Add limit
      sql += ` LIMIT ${limit}`;
      
      // Execute query
      const result = await client.query(sql, params);
      
      // Convert to AirtableRecord objects
      return result.rows.map(row => new AirtableRecord(row, fieldMapping));
      
    } finally {
      client.release();
    }
  }
  
  async _getFieldMapping(client) {
    // Get column names from PostgreSQL
    const result = await client.query(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = $1 AND table_schema IN (${DB_SCHEMA_FILTER})
    `, [this.tableName]);
    
    // Build mapping: PostgreSQL column -> Airtable field name
    // For exact replication, they should be the same (with quotes)
    const mapping = {};
    for (const row of result.rows) {
      mapping[row.column_name] = row.column_name;
    }
    return mapping;
  }
  
  _findColumn(fieldName, fieldMapping) {
    // Find PostgreSQL column for Airtable field name
    for (const [pgCol, airtableField] of Object.entries(fieldMapping)) {
      if (airtableField === fieldName || pgCol === fieldName.toLowerCase().replace(/[^a-z0-9_]/g, '_')) {
        return pgCol;
      }
    }
    return fieldName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  }
  
  /**
   * Parse Airtable filterByFormula to SQL WHERE clause
   * Supports common patterns:
   *   - {field} = "value"
   *   - {field} = 'value'
   *   - AND({field1} = 'value1', {field2} = 'value2')
   *   - SEARCH("value", {field})
   *   - SEARCH("value", ARRAYJOIN({field}))
   *   - {enabled} = TRUE()
   * 
   * Also handles linked record resolution for fields like report_id
   */
  async _parseFilterFormula(formula, fieldMapping, client) {
    if (!formula || formula.trim() === '') {
      return null;
    }
    
    const params = [];
    let paramIndex = 1;
    
    // Handle AND(...) wrapper
    let innerFormula = formula;
    const andMatch = formula.match(/^AND\((.+)\)$/i);
    if (andMatch) {
      innerFormula = andMatch[1];
    }
    
    // Split by comma for AND conditions (simple split, doesn't handle nested)
    const conditions = [];
    
    // Handle SEARCH pattern: SEARCH("value", {field}) or SEARCH("value", ARRAYJOIN({field}))
    const searchPattern = /SEARCH\s*\(\s*["']([^"']+)["']\s*,\s*(?:ARRAYJOIN\s*\(\s*)?\{([^}]+)\}(?:\s*\))?\s*\)/gi;
    let searchMatch;
    let tempFormula = innerFormula;
    
    while ((searchMatch = searchPattern.exec(innerFormula)) !== null) {
      const value = searchMatch[1];
      const field = searchMatch[2];
      const col = this._findColumn(field, fieldMapping);
      
      // For JSONB arrays, use LIKE or @> operator
      conditions.push(`"${col}"::text LIKE $${paramIndex}`);
      params.push(`%${value}%`);
      paramIndex++;
      
      // Remove this match from formula
      tempFormula = tempFormula.replace(searchMatch[0], 'TRUE');
    }
    
    // Handle simple equality: {field} = "value" or {field} = 'value'
    const eqPattern = /\{([^}]+)\}\s*=\s*["']([^"']*)["']/g;
    let eqMatch;
    
    while ((eqMatch = eqPattern.exec(tempFormula)) !== null) {
      const field = eqMatch[1];
      const value = eqMatch[2];
      const col = this._findColumn(field, fieldMapping);
      
      // Try to resolve linked record ID (pass current table name to avoid self-resolution)
      const linkedRecordId = await this._resolveLinkedRecordId(field, value, client, this.tableName);
      
      if (linkedRecordId) {
        // Use LIKE for linked record fields (they store ["recXXX"])
        conditions.push(`"${col}"::text LIKE $${paramIndex}`);
        params.push(`%${linkedRecordId}%`);
        paramIndex++;
      } else {
        // Regular equality or LIKE for text that might be in JSON array
        conditions.push(`("${col}" = $${paramIndex} OR "${col}"::text LIKE $${paramIndex + 1})`);
        params.push(value);
        params.push(`%"${value}"%`);
        paramIndex += 2;
      }
    }
    
    // Handle TRUE() pattern: {field} = TRUE()
    const truePattern = /\{([^}]+)\}\s*=\s*TRUE\(\)/gi;
    let trueMatch;
    
    while ((trueMatch = truePattern.exec(tempFormula)) !== null) {
      const field = trueMatch[1];
      const col = this._findColumn(field, fieldMapping);
      
      conditions.push(`("${col}" = 'true' OR "${col}" = '1' OR "${col}"::boolean = true)`);
    }
    
    if (conditions.length === 0) {
      return null;
    }
    
    return {
      sql: conditions.join(' AND '),
      params: params
    };
  }
  
  /**
   * Resolve a human-readable ID to an Airtable record ID
   * Maps common linked record fields to their source tables
   */
  async _resolveLinkedRecordId(field, value, client, currentTable) {
    // Skip if value already looks like an Airtable record ID
    if (value.startsWith('rec')) {
      return value;
    }
    
    // Map of linked record fields to their source tables and lookup columns
    const LINKED_RECORD_MAP = {
      'report_id': { table: 'reports', lookupCol: 'report_id' },
      'patient_id': { table: 'patients', lookupCol: 'patient_id' },
      'patient': { table: 'patients', lookupCol: 'patient_id' },
      'marker': { table: 'markers', lookupCol: 'marker_id' },
      'marker_link': { table: 'markers', lookupCol: 'marker_id' },
      'client_id': { table: 'partners', lookupCol: 'partner_id' }
    };
    
    const fieldLower = field.toLowerCase();
    const mapping = LINKED_RECORD_MAP[fieldLower];
    
    if (!mapping) {
      return null; // Not a known linked record field
    }
    
    // Don't resolve if we're querying the source table itself
    // e.g., don't resolve report_id when querying reports table
    if (currentTable === mapping.table) {
      return null;
    }
    
    try {
      // Look up the airtable_record_id from the source table
      const result = await client.query(
        `SELECT airtable_record_id FROM "${mapping.table}" WHERE "${mapping.lookupCol}" = $1 LIMIT 1`,
        [value]
      );
      
      if (result.rows.length > 0) {
        console.log(`   🔗 Resolved ${field}="${value}" → ${result.rows[0].airtable_record_id}`);
        return result.rows[0].airtable_record_id;
      }
    } catch (e) {
      // Table or column might not exist, fall back to regular equality
    }
    
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TABLE CLASS
// Handles single table operations
// ═══════════════════════════════════════════════════════════════════════════════

class AirtableTable {
  constructor(adapter, tableName) {
    this.adapter = adapter;
    this.tableName = tableName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  }
  
  select(options = {}) {
    const query = new AirtableQuery(this.adapter, this.tableName);
    return query.select(options);
  }
  
  async find(recordId) {
    const client = await this.adapter.pool.connect();
    
    try {
      const result = await client.query(
        `SELECT * FROM "${this.tableName}" WHERE "airtable_record_id" = $1 LIMIT 1`,
        [recordId]
      );
      
      if (result.rows.length === 0) {
        throw new Error(`Record not found: ${recordId}`);
      }
      
      const fieldMapping = await this._getFieldMapping(client);
      return new AirtableRecord(result.rows[0], fieldMapping);
      
    } finally {
      client.release();
    }
  }
  
  async update(recordId, fields) {
    const client = await this.adapter.pool.connect();
    
    try {
      const setClauses = [];
      const params = [recordId];
      let paramIndex = 2;
      
      for (const [field, value] of Object.entries(fields)) {
        const col = field.toLowerCase().replace(/[^a-z0-9_]/g, '_');
        setClauses.push(`"${col}" = $${paramIndex}`);
        params.push(typeof value === 'object' ? JSON.stringify(value) : value);
        paramIndex++;
      }
      
      if (setClauses.length > 0) {
        await client.query(
          `UPDATE "${this.tableName}" SET ${setClauses.join(', ')} WHERE "airtable_record_id" = $1`,
          params
        );
      }
      
      return this.find(recordId);
      
    } finally {
      client.release();
    }
  }
  
  async create(fields) {
    const client = await this.adapter.pool.connect();
    
    try {
      const columns = ['airtable_record_id', 'airtable_created_time'];
      const values = [this._generateRecordId(), new Date().toISOString()];
      const placeholders = ['$1', '$2'];
      let paramIndex = 3;
      
      for (const [field, value] of Object.entries(fields)) {
        const col = field.toLowerCase().replace(/[^a-z0-9_]/g, '_');
        columns.push(`"${col}"`);
        values.push(typeof value === 'object' ? JSON.stringify(value) : value);
        placeholders.push(`$${paramIndex}`);
        paramIndex++;
      }
      
      const result = await client.query(
        `INSERT INTO "${this.tableName}" (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      
      const fieldMapping = await this._getFieldMapping(client);
      return new AirtableRecord(result.rows[0], fieldMapping);
      
    } finally {
      client.release();
    }
  }
  
  async destroy(recordIds) {
    const client = await this.adapter.pool.connect();
    const ids = Array.isArray(recordIds) ? recordIds : [recordIds];
    
    try {
      const result = await client.query(
        `DELETE FROM "${this.tableName}" WHERE "airtable_record_id" = ANY($1) RETURNING "airtable_record_id"`,
        [ids]
      );
      
      return result.rows.map(row => ({ id: row.airtable_record_id, deleted: true }));
      
    } finally {
      client.release();
    }
  }
  
  async _getFieldMapping(client) {
    const result = await client.query(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = $1 AND table_schema IN (${DB_SCHEMA_FILTER})
    `, [this.tableName]);
    
    const mapping = {};
    for (const row of result.rows) {
      mapping[row.column_name] = row.column_name;
    }
    return mapping;
  }
  
  _generateRecordId() {
    // Generate Airtable-style record ID
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let id = 'rec';
    for (let i = 0; i < 14; i++) {
      id += chars[Math.floor(Math.random() * chars.length)];
    }
    return id;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN ADAPTER CLASS
// Entry point - mimics new Airtable({ apiKey }).base(baseId)
// ═══════════════════════════════════════════════════════════════════════════════

class AirtablePostgresAdapter {
  /**
   * Create adapter instance
   * @param {Object} pgConfig - PostgreSQL connection config
   * @param {string} database - Database name (haq_ontology, haq_scoring, haq_knowledge)
   */
  constructor(pgConfig, database) {
    this.config = {
      ...pgConfig,
      database: database
    };
    this.pool = new Pool(this.config);
    this.database = database;
  }
  
  /**
   * Get table interface (mimics base('tableName'))
   * @param {string} tableName - Table name
   * @returns {AirtableTable} - Table interface
   */
  table(tableName) {
    return new AirtableTable(this, tableName);
  }
  
  /**
   * Callable interface - allows base('tableName') syntax
   * Use: const base = adapter.base(); base('reports').select(...)
   */
  base() {
    const adapter = this;
    return function(tableName) {
      return new AirtableTable(adapter, tableName);
    };
  }
  
  /**
   * Close all connections
   */
  async close() {
    await this.pool.end();
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FACTORY FUNCTION
// Mimics: new Airtable({ apiKey }).base(baseId)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Create Airtable-compatible interface for PostgreSQL
 * 
 * Usage:
 *   const { createBase } = require('./airtable-postgres-adapter');
 *   
 *   // For scoring base:
 *   const scoringBase = createBase('haq_scoring');
 *   const records = await scoringBase('reports').select({...}).firstPage();
 *   
 *   // For ontology base:
 *   const ontologyBase = createBase('haq_ontology');
 */
function createBase(database, pgConfig = null) {
  const config = pgConfig || {
    host: process.env.PG_HOST || 'haq-postgres.cbk4u6eeybm2.us-east-2.rds.amazonaws.com',
    port: process.env.PG_PORT || 5432,
    user: process.env.PG_USER || 'haq_admin',
    password: process.env.PG_PASSWORD || 'HaqSecure2026_1cfa935e',
    ssl: { rejectUnauthorized: false }
  };
  
  const adapter = new AirtablePostgresAdapter(config, database);
  return adapter.base();
}

/**
 * Database mapping from Airtable base IDs to PostgreSQL databases
 */
const BASE_TO_DATABASE = {
  'appwE6FXQqpSz7eRh': 'haq_ontology',   // Ontology
  'appgiPT2PnR2JrVzI': 'haq_scoring',    // Scoring
  'app42HAczcSBeZOxD': 'haq_knowledge'   // Knowledge
};

/**
 * Create base from Airtable base ID (for minimal code changes)
 * 
 * Usage:
 *   const { createBaseFromId } = require('./airtable-postgres-adapter');
 *   const base = createBaseFromId(process.env.AIRTABLE_BASE_ID);
 */
function createBaseFromId(airtableBaseId, pgConfig = null) {
  const database = BASE_TO_DATABASE[airtableBaseId];
  if (!database) {
    throw new Error(`Unknown Airtable base ID: ${airtableBaseId}`);
  }
  return createBase(database, pgConfig);
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  AirtablePostgresAdapter,
  AirtableRecord,
  AirtableTable,
  AirtableQuery,
  createBase,
  createBaseFromId,
  BASE_TO_DATABASE
};
