// Airtable REST API compatible Lambda
const { createBase } = require('./airtable-postgres-adapter');

// Database mapping
const BASE_MAP = {
  'appgiPT2PnR2JrVzI': 'haq_scoring',
  'appwE6FXQqpSz7eRh': 'haq_ontology',
  'app42HAczcSBeZOxD': 'haq_knowledge'
};

exports.handler = async (event) => {
  try {
    // Parse request: /v0/{baseId}/{tableName}
    const path = event.path || event.rawPath;
    const match = path.match(/\/v0\/([^/]+)\/([^/]+)/);
    
    if (!match) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Invalid path' }) };
    }
    
    const [, baseId, tableName] = match;
    const database = BASE_MAP[baseId];
    
    if (!database) {
      return { statusCode: 404, body: JSON.stringify({ error: 'Base not found' }) };
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
      maxRecords: parseInt(queryParams.maxRecords) || 100
    }).all();
    
    // Return in Airtable format
    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
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
      body: JSON.stringify({ error: error.message })
    };
  }
};
