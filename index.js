// Airtable REST API compatible Lambda
const { createBase } = require('./airtable-postgres-adapter');

// Database mapping
const BASE_MAP = {
  'appgiPT2PnR2JrVzI': 'haq_scoring',
  'appwE6FXQqpSz7eRh': 'haq_ontology',
  'app42HAczcSBeZOxD': 'haq_knowledge'
};

// CORS headers
const corsHeaders = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
};

exports.handler = async (event) => {
  // Handle CORS preflight
  const method = event.httpMethod || event.requestContext?.http?.method;
  if (method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: ''
    };
  }

  try {
    // Parse request: /v0/{baseId}/{tableName}
    const path = event.path || event.rawPath;
    const match = path.match(/\/v0\/([^/]+)\/([^/]+)/);
    
    if (!match) {
      return { statusCode: 400, headers: corsHeaders, body: JSON.stringify({ error: 'Invalid path' }) };
    }
    
    const [, baseId, tableName] = match;
    const database = BASE_MAP[baseId];
    
    if (!database) {
      return { statusCode: 404, headers: corsHeaders, body: JSON.stringify({ error: 'Base not found' }) };
    }
    
    // Create base connection
    const base = createBase(database);
    
    // Parse query params
    const queryParams = event.queryStringParameters || {};
    const fields = Object.keys(queryParams)
      .filter(k => k.startsWith('fields['))
      .map(k => queryParams[k]);
    
    // Fetch records
    const records = await base(tableName).select({
      maxRecords: parseInt(queryParams.maxRecords) || 100,
      filterByFormula: queryParams.filterByFormula || null
    }).all();
    
    // Return in Airtable format
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        records: records.map(r => ({
          id: r.id,
          fields: r.fields,
          createdTime: r.createdTime
        }))
      })
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
