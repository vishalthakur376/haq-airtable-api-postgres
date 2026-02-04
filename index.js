/**
 * HAQ Airtable-Compatible REST API for PostgreSQL
 * Version: 3.3 - Auto-create Cognito user when creating users record
 * 
 * ARCHITECTURE NOTE:
 * ==================
 * - All PostgreSQL table structures match Airtable exactly (same column names)
 * - This API uses an Airtable-compatible adapter to maintain feature parity
 * - Supports filterByFormula syntax for seamless frontend migration
 * - Supports Airtable-style pagination with offset
 * - Chat and Portal both use this same adapter pattern
 * 
 * Endpoints:
 * - GET    /v0/{baseId}/{tableName}              - List records (supports pagination)
 * - GET    /v0/{baseId}/{tableName}/{recordId}   - Get single record
 * - POST   /v0/{baseId}/{tableName}              - Create record
 * - PATCH  /v0/{baseId}/{tableName}/{recordId}   - Update record
 * - DELETE /v0/{baseId}/{tableName}/{recordId}   - Delete record
 * - GET    /npi/{npiNumber}                      - Verify NPI (CMS Registry proxy)
 * 
 * Pagination params:
 * - maxRecords: number of records per page (default 1000, max 10000)
 * - offset: pagination cursor for next page
 */

const { Pool } = require('pg');
const https = require('https');
const { CognitoIdentityProviderClient, AdminCreateUserCommand, AdminSetUserPasswordCommand } = require('@aws-sdk/client-cognito-identity-provider');

// Cognito configuration
const COGNITO_USER_POOL_ID = process.env.COGNITO_USER_POOL_ID || 'us-east-2_k0wuHxKK2';
const cognitoClient = new CognitoIdentityProviderClient({ region: 'us-east-2' });

// ═══════════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════════

// Database mapping: Airtable Base ID → PostgreSQL Database
const BASE_MAP = {
  'appgiPT2PnR2JrVzI': 'haq_scoring',
  'appwE6FXQqpSz7eRh': 'haq_ontology',
  'app42HAczcSBeZOxD': 'haq_knowledge'
};

// PostgreSQL connection config
const PG_CONFIG = {
  host: process.env.PG_HOST || 'haq-postgres.cbk4u6eeybm2.us-east-2.rds.amazonaws.com',
  port: process.env.PG_PORT || 5432,
  user: process.env.PG_USER || 'haq_admin',
  password: process.env.PG_PASSWORD || 'HaqSecure2026_1cfa935e',
  ssl: { rejectUnauthorized: false }
};

// Connection pools by database
const pools = {};

function getPool(database) {
  if (!pools[database]) {
    pools[database] = new Pool({ ...PG_CONFIG, database });
  }
  return pools[database];
}

// CORS headers
const corsHeaders = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
};

// ═══════════════════════════════════════════════════════════════════════════════
// NPI Verification (CMS Registry Proxy)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Verify NPI number using CMS NPI Registry API
 * Proxies the request to avoid CORS issues in browser
 */
async function verifyNPI(npiNumber) {
  return new Promise((resolve, reject) => {
    const url = `https://npiregistry.cms.hhs.gov/api/?number=${npiNumber}&version=2.1`;
    
    https.get(url, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve(json);
        } catch (e) {
          reject(new Error('Invalid response from NPI Registry'));
        }
      });
    }).on('error', (err) => {
      reject(err);
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Airtable Record Format
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Convert PostgreSQL row to Airtable record format
 * Preserves exact column names from PostgreSQL (which match Airtable)
 */
function rowToRecord(row) {
  const fields = {};
  
  // Always include the PostgreSQL numeric id in fields for proper identification
  if (row.id) {
    fields.pg_id = row.id;
  }
  
  for (const [key, value] of Object.entries(row)) {
    // Skip internal columns from being duplicated
    if (key === 'id' || key === 'airtable_record_id' || key === 'airtable_created_time') {
      continue;
    }
    
    // Parse JSON strings back to arrays/objects
    let parsedValue = value;
    if (typeof value === 'string' && (value.startsWith('[') || value.startsWith('{'))) {
      try {
        parsedValue = JSON.parse(value);
      } catch (e) {
        // Keep as string if not valid JSON
      }
    }
    
    // v3.2: Convert string booleans to actual booleans for proper JS handling
    // This fixes the "flagged" badge showing for needs_review='false'
    if (parsedValue === 'true') parsedValue = true;
    if (parsedValue === 'false') parsedValue = false;
    
    fields[key] = parsedValue;
  }
  
  return {
    id: row.airtable_record_id || row.id?.toString(),
    fields,
    createdTime: row.airtable_created_time || new Date().toISOString()
  };
}

/**
 * Generate Airtable-style record ID
 */
function generateRecordId() {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let id = 'rec';
  for (let i = 0; i < 14; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Filter Formula Parser
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Parse Airtable filterByFormula to SQL WHERE clause
 * Supports:
 *   - {field} = "value" / {field} = 'value'
 *   - AND({condition1}, {condition2})
 *   - SEARCH("value", {field})
 *   - SEARCH("value", ARRAYJOIN({field}))
 *   - {field} = TRUE()
 */
async function parseFilterFormula(formula, tableName, client) {
  if (!formula || formula.trim() === '') {
    return { sql: '', params: [] };
  }
  
  const params = [];
  let paramIndex = 1;
  const conditions = [];
  
  // Handle AND(...) wrapper
  let innerFormula = formula;
  const andMatch = formula.match(/^AND\((.+)\)$/i);
  if (andMatch) {
    innerFormula = andMatch[1];
  }
  
  // Handle SEARCH pattern
  const searchPattern = /SEARCH\s*\(\s*["']([^"']+)["']\s*,\s*(?:ARRAYJOIN\s*\(\s*)?\{([^}]+)\}(?:\s*\))?\s*\)/gi;
  let searchMatch;
  let tempFormula = innerFormula;
  
  while ((searchMatch = searchPattern.exec(innerFormula)) !== null) {
    const value = searchMatch[1];
    const field = searchMatch[2];
    
    conditions.push(`"${field}"::text ILIKE $${paramIndex}`);
    params.push(`%${value}%`);
    paramIndex++;
    
    tempFormula = tempFormula.replace(searchMatch[0], 'TRUE');
  }
  
  // Handle equality: {field} = "value" or {field} = 'value'
  const eqPattern = /\{([^}]+)\}\s*=\s*["']([^"']*)["']/g;
  let eqMatch;
  
  while ((eqMatch = eqPattern.exec(tempFormula)) !== null) {
    const field = eqMatch[1];
    const value = eqMatch[2];
    
    // Handle JSON array fields (like partner_id which stores ["Formula"])
    // Check both exact match and JSON array containment
    conditions.push(`("${field}" = $${paramIndex} OR "${field}"::text LIKE $${paramIndex + 1})`);
    params.push(value);
    params.push(`%"${value}"%`);
    paramIndex += 2;
  }
  
  // Handle TRUE() pattern
  const truePattern = /\{([^}]+)\}\s*=\s*TRUE\(\)/gi;
  let trueMatch;
  
  while ((trueMatch = truePattern.exec(tempFormula)) !== null) {
    const field = trueMatch[1];
    conditions.push(`("${field}" = 'true' OR "${field}" = '1' OR "${field}"::text = 'true')`);
  }
  
  if (conditions.length === 0) {
    return { sql: '', params: [] };
  }
  
  return {
    sql: 'WHERE ' + conditions.join(' AND '),
    params
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Request Handlers
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * GET /v0/{baseId}/{tableName} - List records with pagination
 * GET /v0/{baseId}/{tableName}/{recordId} - Get single record
 * 
 * Query params:
 * - maxRecords: number of records (default 1000, max 10000)
 * - offset: pagination offset (number)
 * - filterByFormula: Airtable-style filter
 * - sort[0][field]: field to sort by
 * - sort[0][direction]: 'asc' or 'desc'
 */
async function handleGet(pool, tableName, recordId, queryParams) {
  const client = await pool.connect();
  
  try {
    // Normalize table name (lowercase, replace spaces with underscores)
    const normalizedTable = tableName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
    
    if (recordId) {
      // Single record lookup
      const result = await client.query(
        `SELECT * FROM "${normalizedTable}" WHERE "airtable_record_id" = $1 LIMIT 1`,
        [recordId]
      );
      
      if (result.rows.length === 0) {
        return { statusCode: 404, body: { error: 'Record not found' } };
      }
      
      return { statusCode: 200, body: rowToRecord(result.rows[0]) };
    }
    
    // List records with pagination
    let sql = `SELECT * FROM "${normalizedTable}"`;
    let countSql = `SELECT COUNT(*) FROM "${normalizedTable}"`;
    let params = [];
    
    // Parse filterByFormula
    if (queryParams.filterByFormula) {
      const filter = await parseFilterFormula(queryParams.filterByFormula, normalizedTable, client);
      if (filter.sql) {
        sql += ` ${filter.sql}`;
        countSql += ` ${filter.sql}`;
        params = filter.params;
      }
    }
    
    // Add sorting (default to id for consistent pagination)
    if (queryParams['sort[0][field]']) {
      const sortField = queryParams['sort[0][field]'];
      const sortDir = queryParams['sort[0][direction]'] === 'desc' ? 'DESC' : 'ASC';
      sql += ` ORDER BY "${sortField}" ${sortDir} NULLS LAST, "id" ASC`;
    } else {
      sql += ` ORDER BY "id" ASC`;
    }
    
    // Pagination
    const maxRecords = Math.min(parseInt(queryParams.maxRecords) || 1000, 10000);
    const offset = parseInt(queryParams.offset) || 0;
    
    sql += ` LIMIT ${maxRecords + 1}`; // Fetch one extra to check if there are more
    if (offset > 0) {
      sql += ` OFFSET ${offset}`;
    }
    
    console.log(`[GET] ${normalizedTable}: ${sql} (offset: ${offset})`);
    
    // Execute query
    const result = await client.query(sql, params);
    
    // Check if there are more records
    const hasMore = result.rows.length > maxRecords;
    const records = hasMore ? result.rows.slice(0, maxRecords) : result.rows;
    
    // Build response
    const response = {
      records: records.map(rowToRecord)
    };
    
    // Add offset for next page if there are more records
    if (hasMore) {
      response.offset = (offset + maxRecords).toString();
    }
    
    return {
      statusCode: 200,
      body: response
    };
    
  } finally {
    client.release();
  }
}

/**
 * POST /v0/{baseId}/{tableName} - Create record
 */
async function handlePost(pool, tableName, body) {
  const client = await pool.connect();
  
  try {
    const normalizedTable = tableName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
    const fields = body.fields || body;
    
    // v3.3: If creating a user with email/password, also create Cognito user
    if (normalizedTable === 'users' && fields.email && fields.password) {
      try {
        console.log(`[COGNITO] Creating user: ${fields.email}`);
        
        // Create Cognito user
        const createCommand = new AdminCreateUserCommand({
          UserPoolId: COGNITO_USER_POOL_ID,
          Username: fields.email,
          UserAttributes: [
            { Name: 'email', Value: fields.email },
            { Name: 'email_verified', Value: 'true' }
          ],
          MessageAction: 'SUPPRESS' // Don't send welcome email
        });
        await cognitoClient.send(createCommand);
        
        // Set permanent password
        const passwordCommand = new AdminSetUserPasswordCommand({
          UserPoolId: COGNITO_USER_POOL_ID,
          Username: fields.email,
          Password: fields.password,
          Permanent: true
        });
        await cognitoClient.send(passwordCommand);
        
        console.log(`[COGNITO] User created successfully: ${fields.email}`);
      } catch (cognitoError) {
        console.error(`[COGNITO] Error creating user: ${cognitoError.message}`);
        // If user already exists, continue with DB creation
        if (cognitoError.name !== 'UsernameExistsException') {
          throw new Error(`Cognito error: ${cognitoError.message}`);
        }
        console.log(`[COGNITO] User already exists, continuing with DB record`);
      }
    }
    
    // Build INSERT statement (exclude password from DB storage)
    const columns = ['airtable_record_id', 'airtable_created_time'];
    const values = [generateRecordId(), new Date().toISOString()];
    const placeholders = ['$1', '$2'];
    let paramIndex = 3;
    
    for (const [field, value] of Object.entries(fields)) {
      // Don't store plain password in database
      if (field === 'password') continue;
      
      columns.push(`"${field}"`);
      values.push(typeof value === 'object' ? JSON.stringify(value) : value);
      placeholders.push(`$${paramIndex}`);
      paramIndex++;
    }
    
    const sql = `INSERT INTO "${normalizedTable}" (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`;
    console.log(`[POST] ${normalizedTable}: Creating record`);
    
    const result = await client.query(sql, values);
    
    return {
      statusCode: 200,
      body: rowToRecord(result.rows[0])
    };
    
  } finally {
    client.release();
  }
}

/**
 * PATCH /v0/{baseId}/{tableName}/{recordId} - Update record
 */
async function handlePatch(pool, tableName, recordId, body) {
  const client = await pool.connect();
  
  try {
    const normalizedTable = tableName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
    const fields = body.fields || body;
    
    // Build UPDATE statement
    const setClauses = [];
    const params = [recordId];
    let paramIndex = 2;
    
    for (const [field, value] of Object.entries(fields)) {
      setClauses.push(`"${field}" = $${paramIndex}`);
      params.push(typeof value === 'object' ? JSON.stringify(value) : value);
      paramIndex++;
    }
    
    if (setClauses.length === 0) {
      return { statusCode: 400, body: { error: 'No fields to update' } };
    }
    
    const sql = `UPDATE "${normalizedTable}" SET ${setClauses.join(', ')} WHERE "airtable_record_id" = $1 RETURNING *`;
    console.log(`[PATCH] ${normalizedTable}/${recordId}: Updating ${setClauses.length} fields`);
    
    const result = await client.query(sql, params);
    
    if (result.rows.length === 0) {
      return { statusCode: 404, body: { error: 'Record not found' } };
    }
    
    return {
      statusCode: 200,
      body: rowToRecord(result.rows[0])
    };
    
  } finally {
    client.release();
  }
}

/**
 * DELETE /v0/{baseId}/{tableName}/{recordId} - Delete record
 */
async function handleDelete(pool, tableName, recordId) {
  const client = await pool.connect();
  
  try {
    const normalizedTable = tableName.toLowerCase().replace(/[^a-z0-9_]/g, '_');
    
    const result = await client.query(
      `DELETE FROM "${normalizedTable}" WHERE "airtable_record_id" = $1 RETURNING "airtable_record_id"`,
      [recordId]
    );
    
    if (result.rows.length === 0) {
      return { statusCode: 404, body: { error: 'Record not found' } };
    }
    
    console.log(`[DELETE] ${normalizedTable}/${recordId}: Deleted`);
    
    return {
      statusCode: 200,
      body: { id: recordId, deleted: true }
    };
    
  } finally {
    client.release();
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lambda Handler
// ═══════════════════════════════════════════════════════════════════════════════

exports.handler = async (event) => {
  const method = event.httpMethod || event.requestContext?.http?.method;
  
  // Handle CORS preflight
  if (method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: ''
    };
  }

  try {
    const path = event.path || event.rawPath;
    
    // ═══════════════════════════════════════════════════════════════════════════
    // NPI Verification Endpoint: GET /npi/{npiNumber}
    // ═══════════════════════════════════════════════════════════════════════════
    const npiMatch = path.match(/\/npi\/(\d{10})$/);
    if (npiMatch && method === 'GET') {
      const npiNumber = npiMatch[1];
      console.log(`[NPI] Verifying NPI: ${npiNumber}`);
      
      try {
        const npiData = await verifyNPI(npiNumber);
        return {
          statusCode: 200,
          headers: corsHeaders,
          body: JSON.stringify(npiData)
        };
      } catch (err) {
        console.error('[NPI] Error:', err);
        return {
          statusCode: 502,
          headers: corsHeaders,
          body: JSON.stringify({ error: 'Failed to verify NPI', details: err.message })
        };
      }
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // Standard Airtable-compatible API: /v0/{baseId}/{tableName}
    // ═══════════════════════════════════════════════════════════════════════════
    const match = path.match(/\/v0\/([^/]+)\/([^/]+)(?:\/([^/]+))?/);
    
    if (!match) {
      return {
        statusCode: 400,
        headers: corsHeaders,
        body: JSON.stringify({ error: 'Invalid path. Expected: /v0/{baseId}/{tableName} or /npi/{npiNumber}' })
      };
    }
    
    const [, baseId, tableName, recordId] = match;
    const database = BASE_MAP[baseId];
    
    if (!database) {
      return {
        statusCode: 404,
        headers: corsHeaders,
        body: JSON.stringify({ error: `Base not found: ${baseId}` })
      };
    }
    
    // Get database pool
    const pool = getPool(database);
    
    // Parse query params and body
    const queryParams = event.queryStringParameters || {};
    let body = {};
    if (event.body) {
      try {
        body = JSON.parse(event.body);
      } catch (e) {
        return {
          statusCode: 400,
          headers: corsHeaders,
          body: JSON.stringify({ error: 'Invalid JSON body' })
        };
      }
    }
    
    // Route to appropriate handler
    let result;
    
    switch (method) {
      case 'GET':
        result = await handleGet(pool, tableName, recordId, queryParams);
        break;
      case 'POST':
        result = await handlePost(pool, tableName, body);
        break;
      case 'PATCH':
      case 'PUT':
        if (!recordId) {
          return {
            statusCode: 400,
            headers: corsHeaders,
            body: JSON.stringify({ error: 'Record ID required for update' })
          };
        }
        result = await handlePatch(pool, tableName, recordId, body);
        break;
      case 'DELETE':
        if (!recordId) {
          return {
            statusCode: 400,
            headers: corsHeaders,
            body: JSON.stringify({ error: 'Record ID required for delete' })
          };
        }
        result = await handleDelete(pool, tableName, recordId);
        break;
      default:
        return {
          statusCode: 405,
          headers: corsHeaders,
          body: JSON.stringify({ error: `Method not allowed: ${method}` })
        };
    }
    
    return {
      statusCode: result.statusCode,
      headers: corsHeaders,
      body: JSON.stringify(result.body)
    };
    
  } catch (error) {
    console.error('Error:', error);
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({ error: error.message })
    };
  }
};
