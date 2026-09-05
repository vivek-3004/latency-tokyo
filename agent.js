import dgram from 'dgram';
import https from 'https';
import http from 'http';
import { performance } from 'perf_hooks';
import dns from 'dns/promises';
import tls from 'tls';

// ─── CONFIG ───
const NODE_ID = process.env.NODE_ID || 'tokyo';
const REGION = process.env.REGION || 'Asia';
const CENTRAL_WS = process.env.CENTRAL_WS || 'wss://latency-central.onrender.com/ws';
const PROBE_INTERVAL = parseInt(process.env.PROBE_INTERVAL || '10000'); // 10s
const PORT = process.env.PORT || 10000;

// ─── PEER NODES TO PROBE ───
const PEERS = [
  'mumbai', 'delhi', 'singapore', 'tokyo', 'sydney',
  'dubai', 'london', 'frankfurt', 'virginia', 'california',
  'brazil', 'southafrica'
].filter(p => p !== NODE_ID);

// Probe targets (HTTP endpoints on peer nodes)
const PROBE_TARGETS = PEERS.map(peer => ({
  id: peer,
  host: `latency-${peer}.onrender.com`,
  port: 443,
  path: '/probe'
}));

// ─── HTTP SERVER (REQUIRED BY RENDER) ───
const server = http.createServer((req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // Root — status page
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      service: 'latency-probe',
      nodeId: NODE_ID,
      region: REGION,
      uptime: process.uptime(),
      wsConnected: ws && ws.readyState === 1,
      lastProbes: probeHistory.slice(-5),
      endpoints: {
        root: '/',
        health: '/health',
        probe: '/probe',
        stats: '/stats'
      }
    }, null, 2));
    return;
  }

  // Probe endpoint — used by OTHER nodes to measure latency TO this node
  if (req.url === '/probe') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      nodeId: NODE_ID,
      region: REGION,
      timestamp: Date.now()
    }));
    return;
  }

  // Stats endpoint
  if (req.url === '/stats') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      nodeId: NODE_ID,
      region: REGION,
      totalProbes: probeHistory.length,
      avgLatency: avgLatency(),
      uptime: process.uptime(),
      wsConnected: ws && ws.readyState === 1
    }, null, 2));
    return;
  }

  // 404
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ 
    error: 'Not Found',
    nodeId: NODE_ID,
    endpoints: ['/', '/health', '/probe', '/stats']
  }));
});

// ─── WEBSOCKET CLIENT (CONNECTS TO CENTRAL) ───
let ws = null;
let reconnectAttempts = 0;
const probeHistory = [];

function connectWS() {
  console.log(`[${NODE_ID}] Connecting to ${CENTRAL_WS}...`);
  ws = new WebSocket(CENTRAL_WS);

  ws.onopen = () => {
    console.log(`[${NODE_ID}] ✓ Connected to central`);
    reconnectAttempts = 0;
    
    // Register as a node
    ws.send(JSON.stringify({
      type: 'register-node',
      nodeId: NODE_ID,
      region: REGION,
      peers: PEERS
    }));
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'pong') {
        // Heartbeat response
      }
    } catch (e) {}
  };

  ws.onclose = () => {
    console.log(`[${NODE_ID}] ✗ Disconnected, reconnecting...`);
    const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
    reconnectAttempts++;
    setTimeout(connectWS, delay);
  };

  ws.onerror = (err) => {
    console.error(`[${NODE_ID}] WS error:`, err.message);
  };
}

// ─── PROBE FUNCTIONS ───
async function probeHTTP(target) {
  const start = performance.now();
  const dnsStart = performance.now();
  
  try {
    // DNS lookup
    const dnsResult = await dns.lookup(target.host);
    const dnsTime = performance.now() - dnsStart;
    
    // HTTPS request
    const tlsStart = performance.now();
    const result = await httpsRequest(target);
    const tlsTime = performance.now() - tlsStart;
    
    const totalTime = performance.now() - start;
    
    return {
      from: NODE_ID,
      to: target.id,
      latency: totalTime,
      dns: dnsTime,
      tls: { time: tlsTime, valid: result.valid },
      timestamp: Date.now(),
      status: 'success'
    };
  } catch (err) {
    return {
      from: NODE_ID,
      to: target.id,
      latency: -1,
      dns: -1,
      tls: { valid: false },
      timestamp: Date.now(),
      status: 'error',
      error: err.message
    };
  }
}

function httpsRequest(target) {
  return new Promise((resolve, reject) => {
    const start = performance.now();
    const req = https.request({
      host: target.host,
      port: target.port,
      path: target.path,
      method: 'GET',
      timeout: 5000,
      agent: new https.Agent({
        rejectUnauthorized: false, // Render uses valid certs anyway
        keepAlive: true
      })
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode === 200) {
          resolve({ valid: true, statusCode: res.statusCode });
        } else {
          reject(new Error(`HTTP ${res.statusCode}`));
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('timeout'));
    });
    req.end();
  });
}

// ─── PROBE LOOP ───
async function runProbes() {
  console.log(`[${NODE_ID}] Probing ${PROBE_TARGETS.length} peers...`);
  
  const results = await Promise.all(
    PROBE_TARGETS.map(target => probeHTTP(target))
  );

  results.forEach(result => {
    probeHistory.push(result);
    if (probeHistory.length > 100) probeHistory.shift();
    
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({
        type: 'probe-result',
        ...result
      }));
    }
    
    if (result.latency > 0) {
      console.log(`[${NODE_ID}] → ${result.to}: ${result.latency.toFixed(1)}ms`);
    } else {
      console.log(`[${NODE_ID}] → ${result.to}: FAILED (${result.error})`);
    }
  });
}

function avgLatency() {
  const valid = probeHistory.filter(p => p.latency > 0);
  if (valid.length === 0) return 0;
  return valid.reduce((a, b) => a + b.latency, 0) / valid.length;
}

// ─── HEARTBEAT ───
setInterval(() => {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify({ type: 'ping' }));
  }
}, 30000);

// ─── START ───
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Probe node [${NODE_ID}] running on port ${PORT}`);
  console.log(`   Region: ${REGION}`);
  console.log(`   Peers: ${PEERS.length}`);
  console.log(`   Central: ${CENTRAL_WS}`);
});

connectWS();

// Initial probe after 2s (let WS connect first)
setTimeout(runProbes, 2000);

// Probe loop
setInterval(runProbes, PROBE_INTERVAL);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down...');
  server.close(() => process.exit(0));
});
