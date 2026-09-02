// agent.js — Deploy as 12 separate Web Services on Render
// Each with different NODE_ID env var

import https from 'https';
import dns from 'dns/promises';
import net from 'net';
import { WebSocket } from 'ws';
import { createServer } from 'http'; 


// ─── CONFIG FROM ENV ───
const NODE_ID = process.env.NODE_ID || 'tokyo';
const CENTRAL_URL = process.env.CENTRAL_WS_URL || 'wss://latency-central.onrender.com';
const PEERS = JSON.parse(process.env.PEER_IDS || `["mumbai","delhi","singapore","sydney","dubai","london","frankfurt","virginia","california","brazil","southafrica"]`);
const PROBE_INTERVAL = parseInt(process.env.PROBE_INTERVAL || '10000'); // 10s
const PROBE_TIMEOUT = 3000;
const PORT = process.env.PORT || 10000;

console.log(`[${NODE_ID}] Starting — Peers: ${PEERS.join(', ')}`);
console.log(`[${NODE_ID}] Central: ${CENTRAL_URL}`);

// ─── STATE ───
let ws = null;
let reconnectTimer = null;
let probeTimer = null;

// ─── WEBSOCKET CONNECTION ───
function connect() {
  ws = new WebSocket(CENTRAL_URL);

  ws.on('open', () => {
    console.log(`[${NODE_ID}] Connected to central`);
    ws.send(JSON.stringify({ type: 'register-node', nodeId: NODE_ID }));
    
    // Start probing
    if (probeTimer) clearInterval(probeTimer);
    probeTimer = setInterval(probeAllPeers, PROBE_INTERVAL);
    probeAllPeers(); // immediate first probe
  });

  ws.on('close', () => {
    console.log(`[${NODE_ID}] Disconnected, reconnecting in 5s...`);
    if (probeTimer) clearInterval(probeTimer);
    reconnectTimer = setTimeout(connect, 5000);
  });

  ws.on('error', (err) => {
    console.error(`[${NODE_ID}] WS Error:`, err.message);
  });
}

connect();

// ─── HEALTH CHECK HTTP SERVER (required by Render) ───
const healthServer = createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
      status: 'ok', 
      nodeId: NODE_ID,
      centralConnected: ws?.readyState === WebSocket.OPEN,
      uptime: process.uptime()
    }));
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

healthServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[${NODE_ID}] Health server listening on port ${PORT}`);
});

// ─── UPDATE GRACEFUL SHUTDOWN ███
process.on('SIGTERM', () => {
  console.log(`[${NODE_ID}] SIGTERM received, shutting down...`);
  if (probeTimer) clearInterval(probeTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (ws) ws.close();
  healthServer.close(() => process.exit(0));  // ███ CHANGED
});

// ─── GRACEFUL SHUTDOWN (Render sends SIGTERM) ───
process.on('SIGTERM', () => {
  console.log(`[${NODE_ID}] SIGTERM received, shutting down...`);
  if (probeTimer) clearInterval(probeTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (ws) ws.close();
  process.exit(0);
});

// ─── PROBE ALL PEERS ───
async function probeAllPeers() {
  for (const peer of PEERS) {
    if (peer === NODE_ID) continue;
    
    const peerHost = `${peer}.onrender.com`; // Render default domain pattern
    const result = await measurePeer(peer, peerHost);
    
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'probe-result',
        from: NODE_ID,
        to: peer,
        timestamp: Date.now(),
        ...result
      }));
    }
  }
}

// ─── MEASURE SINGLE PEER ───
async function measurePeer(peerId, host) {
  const results = {};

  // Run all probes in parallel
  const [latency, dns, tls, http] = await Promise.allSettled([
    tcpPing(host, 443),
    measureDNS(host),
    measureTLS(host),
    measureHTTP(`https://${host}`)
  ]);

  results.latency = latency.status === 'fulfilled' ? latency.value : -1;
  results.dns = dns.status === 'fulfilled' ? dns.value : -1;
  results.tls = tls.status === 'fulfilled' ? tls.value : { handshakeMs: -1, valid: false };
  results.http = http.status === 'fulfilled' ? http.value : { status: 0, ms: -1 };

  return results;
}

// ─── TCP PING (Latency) ───
function tcpPing(host, port) {
  return new Promise((resolve) => {
    const start = process.hrtime.bigint();
    const socket = new net.Socket();
    socket.setTimeout(PROBE_TIMEOUT);
    
    socket.connect(port, host, () => {
      const ms = Number(process.hrtime.bigint() - start) / 1e6;
      socket.destroy();
      resolve(Math.round(ms * 10) / 10); // 0.1ms precision
    });
    
    socket.on('error', () => resolve(-1));
    socket.on('timeout', () => { socket.destroy(); resolve(-1); });
  });
}

// ─── DNS RESOLUTION TIME ───
async function measureDNS(host) {
  const start = Date.now();
  try {
    await dns.resolve4(host);
    return Date.now() - start;
  } catch {
    return -1;
  }
}

// ─── TLS HANDSHAKE + CERT ───
function measureTLS(host) {
  return new Promise((resolve) => {
    const start = Date.now();
    const req = https.request({
      hostname: host,
      port: 443,
      method: 'HEAD',
      timeout: PROBE_TIMEOUT,
      rejectUnauthorized: false // We check cert ourselves
    }, (res) => {
      const cert = res.socket.getPeerCertificate();
      const valid = res.socket.authorized && cert && cert.valid_to;
      const daysLeft = valid ? Math.ceil((new Date(cert.valid_to) - Date.now()) / 86400000) : -1;
      
      resolve({
        handshakeMs: Date.now() - start,
        valid,
        issuer: cert.issuer?.O || 'unknown',
        daysUntilExpiry: daysLeft
      });
    });
    
    req.on('error', () => resolve({ handshakeMs: -1, valid: false, issuer: 'error', daysUntilExpiry: -1 }));
    req.on('timeout', () => { req.destroy(); resolve({ handshakeMs: -1, valid: false, issuer: 'timeout', daysUntilExpiry: -1 }); });
    req.end();
  });
}

// ─── HTTP REACHABILITY ───
function measureHTTP(url) {
  return new Promise((resolve) => {
    const start = Date.now();
    const req = https.get(url, { timeout: PROBE_TIMEOUT }, (res) => {
      let data = '';
      res.on('data', () => {});
      res.on('end', () => {
        resolve({ status: res.statusCode, ms: Date.now() - start });
      });
    });
    req.on('error', () => resolve({ status: 0, ms: -1 }));
    req.on('timeout', () => { req.destroy(); resolve({ status: 0, ms: -1 }); });
  });
}
