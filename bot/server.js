"use strict";

/**
 * CART3R.JAILBREAK — Helius scanner + wallet-signed Jupiter Swap V2 API.
 * - Scanner and candidate analysis remain read-only.
 * - Real swaps require a fresh Jupiter order and an explicit Phantom wallet signature.
 * - The server never stores private keys or signs transactions for users.
 * - Uses Helius logsSubscribe WebSockets and confirmed swap transaction decoding.
 *
 * Important: activity/volume below is ONLY what this process has observed since it started.
 * Verified CPMM quote-side reserves are read on-chain; estimated total liquidity is labeled as an estimate.
 * LaunchLab curve-only candidates remain ineligible until a verified Raydium CPMM pool is observed.
 */

const express = require("express");
const crypto = require("crypto");
const app = express();
app.use(express.json({ limit: "100kb" }));
// Public read-only CORS for the GitHub Pages dashboard.
// Only GET/OPTIONS are allowed; this does not expose trading or write endpoints.
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

const PORT = Number(process.env.PORT || 3000);
const HELIUS_RPC_URL = String(process.env.HELIUS_RPC_URL || "").trim();
const JUPITER_API_KEY = String(process.env.JUPITER_API_KEY || "").trim();

const SOL_MINT = "So11111111111111111111111111111111111111112";
const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const RAYDIUM_CPMM_PROGRAM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

// Official mainnet program addresses. We listen for actual transactions, not market-indexer data.
const DEX_PROGRAMS = [
  // Focus on new-token launch activity and Raydium's constant-product pools.
  { name: "Raydium LaunchLab", id: "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj" },
  { name: "Raydium CPMM", id: "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C" }
];

const CONFIG = {
  maxCandidates: 100,
  windowMs: 15 * 60 * 1000,
  checkEveryMs: 45 * 1000,
  checkBatch: 5,
  minSwaps: 4,
  minBuyers: 2,
  minBuyPressure: 0.52,
  maxRoundTripLossPct: 9,
  buyTestLamports: "10000000", // 0.01 SOL — quote only, no order execution
  maxSeenSignatures: 5000,
  maxQueuedSignatures: 500,
  maxConcurrentTransactions: 2,
  minLiquiditySol: 3,
  quoteLadderLamports: ["10000000", "50000000"], // 0.01 and 0.05 SOL, quote-only
  rpcTimeoutMs: 12000,
  jupiterTimeoutMs: 12000
};

const state = {
  running: true,
  mode: "TEST",
  realTrading: false,
  scanner: "STARTING",
  pools: new Map(),
  seenSignatures: new Set(),
  signatureQueue: [],
  processing: 0,
  ws: null,
  wsReconnectTimer: null,
  checkTimer: null,
  lastUpdate: null,
  lastError: null,
  scanning: false,
  nextScanInSeconds: 0,
  startedAt: new Date().toISOString(),
  helius: {
    configured: Boolean(HELIUS_RPC_URL),
    connected: false,
    subscriptions: 0,
    lastCheck: null,
    lastSlot: null,
    lastError: null,
    transactionsSeen: 0,
    swapsDecoded: 0,
    reconnects: 0,
    rpc429Retries: 0,
    droppedQueuedTransactions: 0
  },
  jupiter: {
    configured: Boolean(JUPITER_API_KEY),
    api: "SWAP_V2_ORDER",
    lastCheck: null,
    lastRequestAt: null,
    lastSolPriceUsd: null,
    lastSolPriceAt: 0,
    checksCount: 0,
    successCount: 0,
    rateLimitHits: 0,
    cooldownSeconds: 0,
    lastError: null
  },
  discovery: {
    provider: "HELIUS_LOGS_WEBSOCKET",
    feeds: DEX_PROGRAMS.length,
    currentFeed: null,
    callsCount: 0,
    successCount: 0,
    failureCount: 0,
    lastCheck: null,
    lastError: null
  },
  protections: {
    mintAuthorityCheck: true,
    freezeAuthorityCheck: true,
    tokenProgramCheck: true,
    token2022ExtensionInspection: false,
    poolAddressDiscovery: true,
    liquidityCheck: true,
    observedSwapActivityCheck: true,
    buyPressureCheck: true,
    volumePressureCheck: true,
    scoreCheck: true,
    buyQuoteCheck: true,
    sellQuoteCheck: true,
    quoteLadderCheck: true,
    sellabilitySimulation: false,
    realTrading: false
  }
};

const now = () => Date.now();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const toNum = v => Number.isFinite(Number(v)) ? Number(v) : 0;
const round = (n, d = 2) => Number.isFinite(n) ? Number(n.toFixed(d)) : null;
const safeError = e => String(e && e.message ? e.message : e).slice(0, 350);

function rpcWebSocketUrl() {
  if (!HELIUS_RPC_URL) return null;
  try {
    const u = new URL(HELIUS_RPC_URL);
    u.protocol = "wss:";
    // Helius standard mainnet RPC URLs work as the matching websocket endpoint.
    if (!u.hostname.includes("helius")) {
      const key = u.searchParams.get("api-key");
      const fallback = new URL("wss://mainnet.helius-rpc.com/");
      if (key) fallback.searchParams.set("api-key", key);
      return fallback.toString();
    }
    return u.toString();
  } catch (_) {
    return null;
  }
}

async function fetchJson(url, options = {}, timeoutMs = CONFIG.rpcTimeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const body = await response.text();
    let json = {};
    try { json = body ? JSON.parse(body) : {}; } catch (_) {}
    if (!response.ok) {
      const detail = json.error?.message || body.slice(0, 160) || response.statusText;
      const error = new Error("HTTP " + response.status + ": " + detail);
      error.status = response.status;
      const retryAfter = response.headers.get("retry-after");
      const resetHeader = response.headers.get("x-ratelimit-reset");
      error.retryAfterMs = retryAfter
        ? (Number.isFinite(Number(retryAfter)) ? Number(retryAfter) * 1000 : Math.max(0, Date.parse(retryAfter) - Date.now()))
        : 0;
      error.rateLimitResetAt = resetHeader && Number.isFinite(Number(resetHeader))
        ? Number(resetHeader) * 1000
        : 0;
      error.rateLimitRemaining = Number(response.headers.get("x-ratelimit-remaining"));
      throw error;
    }
    if (body && !Object.keys(json).length) throw new Error("Réponse JSON invalide (HTTP " + response.status + ")");
    if (json && typeof json === "object") {
      const resetHeader = response.headers.get("x-ratelimit-reset");
      Object.defineProperty(json, "__rateLimit", {
        value: {
          remaining: Number(response.headers.get("x-ratelimit-remaining")),
          resetAt: resetHeader && Number.isFinite(Number(resetHeader)) ? Number(resetHeader) * 1000 : 0
        },
        enumerable: false
      });
    }
    return json;
  } finally {
    clearTimeout(timer);
  }
}

// Serialize Helius HTTP RPC calls to avoid bursts that trigger HTTP 429.
let heliusRpcTail = Promise.resolve();
let lastHeliusRpcAt = 0;
let heliusCooldownUntil = 0;
async function executeHeliusRpc(method, params) {
  if (!HELIUS_RPC_URL) throw new Error("HELIUS_RPC_URL non configurée");
  for (let attempt = 0; attempt < 4; attempt++) {
    const wait = Math.max(heliusCooldownUntil - Date.now(), 350 - (Date.now() - lastHeliusRpcAt), 0);
    if (wait) await sleep(wait);
    lastHeliusRpcAt = Date.now();
    try {
      const json = await fetchJson(HELIUS_RPC_URL, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: Date.now(), method, params })
      });
      if (json.error) throw new Error(json.error.message || "Erreur RPC Helius");
      return json.result;
    } catch (e) {
      const limited = e?.status === 429 || /429|rate.?limit|too many requests/i.test(safeError(e));
      const transient = e?.status >= 500 && e?.status <= 599;
      if ((!limited && !transient) || attempt === 3) throw e;
      const delay = limited && e?.retryAfterMs > 0 ? Math.min(e.retryAfterMs, 30000) : 1200 * Math.pow(2, attempt);
      if (limited) {
        state.helius.rpc429Retries++;
        heliusCooldownUntil = Math.max(heliusCooldownUntil, Date.now() + delay);
      }
      await sleep(delay);
    }
  }
}
function heliusRpc(method, params) {
  const task = heliusRpcTail.then(
    () => executeHeliusRpc(method, params),
    () => executeHeliusRpc(method, params)
  );
  heliusRpcTail = task.catch(() => {});
  return task;
}

function mintBalanceMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    if (!row || !row.mint || !row.owner) continue;
    const amount = row.uiTokenAmount && row.uiTokenAmount.uiAmount != null
      ? Number(row.uiTokenAmount.uiAmount)
      : Number(row.uiTokenAmount?.amount || 0) / Math.pow(10, Number(row.uiTokenAmount?.decimals || 0));
    const key = row.owner + "|" + row.mint;
    map.set(key, (map.get(key) || 0) + (Number.isFinite(amount) ? amount : 0));
  }
  return map;
}

function accountKeyObjects(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  const header = tx?.transaction?.message?.header || {};
  const required = Number(header.numRequiredSignatures || 0);
  return keys.map((k, i) => {
    if (typeof k === "string") return { pubkey: k, signer: i < required, index: i };
    return { pubkey: k.pubkey, signer: Boolean(k.signer) || i < required, index: i };
  });
}

// Anchor instruction data is base58. Decode it so pool discovery only accepts
// the two documented CPMM swap instructions, not another 13-account instruction.
function decodeBase58(value) {
  const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let number = 0n;
  for (const char of String(value || "")) {
    const digit = alphabet.indexOf(char);
    if (digit < 0) return Buffer.alloc(0);
    number = number * 58n + BigInt(digit);
  }
  const bytes = [];
  while (number > 0n) {
    bytes.unshift(Number(number & 255n));
    number >>= 8n;
  }
  const leadingZeroes = (String(value).match(/^1*/) || [""])[0].length;
  return Buffer.concat([Buffer.alloc(leadingZeroes), Buffer.from(bytes)]);
}
const CPMM_SWAP_DISCRIMINATORS = new Set([
  crypto.createHash("sha256").update("global:swap_base_input").digest().subarray(0, 8).toString("hex"),
  crypto.createHash("sha256").update("global:swap_base_output").digest().subarray(0, 8).toString("hex")
]);

// Raydium CPMM SwapBaseInput/Output uses a documented 13-account layout:
// [3] pool_state, [6] input_vault, [7] output_vault, [10] input_mint, [11] output_mint.
// Only report a pool when a matching swap discriminator and CPMM instruction are present.
function extractRaydiumCpmmMarket(tx, mint) {
  const keys = accountKeyObjects(tx);
  const all = [
    ...(tx?.transaction?.message?.instructions || []),
    ...((tx?.meta?.innerInstructions || []).flatMap(x => x.instructions || []))
  ];
  for (const ix of all) {
    const programId = ix.programId || keys[Number(ix.programIdIndex)]?.pubkey;
    if (programId !== RAYDIUM_CPMM_PROGRAM) continue;
    const data = decodeBase58(ix.data);
    if (data.length < 8 || !CPMM_SWAP_DISCRIMINATORS.has(data.subarray(0, 8).toString("hex"))) continue;
    const accounts = (ix.accounts || []).map(a =>
      typeof a === "number" ? keys[a]?.pubkey : (typeof a === "string" ? a : a?.pubkey)
    );
    if (accounts.length < 13) continue;
    const inputMint = accounts[10];
    const outputMint = accounts[11];
    if (inputMint !== mint && outputMint !== mint) continue;
    const baseIsInput = inputMint === mint;
    const market = {
      poolAddress: accounts[3],
      poolProgramId: RAYDIUM_CPMM_PROGRAM,
      baseVault: baseIsInput ? accounts[6] : accounts[7],
      quoteVault: baseIsInput ? accounts[7] : accounts[6],
      quoteMint: baseIsInput ? outputMint : inputMint
    };
    if (market.poolAddress && market.baseVault && market.quoteVault &&
        [SOL_MINT, USDC_MINT, USDT_MINT].includes(market.quoteMint)) return market;
  }
  return null;
}

function findSwapForSigner(tx) {
  const meta = tx?.meta;
  if (!meta || meta.err) return null;
  const keys = accountKeyObjects(tx);
  const signers = keys.filter(k => k.signer && k.pubkey);
  if (!signers.length) return null;
  const pre = mintBalanceMap(meta.preTokenBalances);
  const post = mintBalanceMap(meta.postTokenBalances);
  const allKeys = new Set([...pre.keys(), ...post.keys()]);
  const solMintSet = new Set([SOL_MINT, USDC_MINT, USDT_MINT]);
  let best = null;

  for (const signer of signers) {
    const deltas = [];
    for (const key of allKeys) {
      const split = key.split("|");
      if (split[0] !== signer.pubkey) continue;
      const mint = split.slice(1).join("|");
      const delta = (post.get(key) || 0) - (pre.get(key) || 0);
      if (Math.abs(delta) > 0) deltas.push({ mint, delta });
    }
    const tokenChanges = deltas.filter(x => !solMintSet.has(x.mint));
    if (!tokenChanges.length) continue;

    const account = meta.preBalances && meta.postBalances ? signer.index : -1;
    let solDelta = 0;
    if (account >= 0 && meta.preBalances[account] != null && meta.postBalances[account] != null) {
      solDelta = (Number(meta.postBalances[account]) - Number(meta.preBalances[account])) / 1e9;
    }
    const stableChange = deltas.find(x => x.mint === USDC_MINT || x.mint === USDT_MINT);
    const wrappedSolChange = deltas.find(x => x.mint === SOL_MINT);
    let quoteDeltaUsd = 0;
    let quoteKind = "SOL";
    if (stableChange) {
      quoteDeltaUsd = stableChange.delta;
      quoteKind = "USD_STABLE";
    } else if (wrappedSolChange) {
      quoteDeltaUsd = wrappedSolChange.delta;
      quoteKind = "SOL";
    } else {
      quoteDeltaUsd = solDelta;
      quoteKind = "SOL";
    }
    if (Math.abs(quoteDeltaUsd) < (quoteKind === "SOL" ? 0.00005 : 0.01)) continue;

    for (const token of tokenChanges) {
      // Buy: wallet receives token while spending SOL/stablecoin. Sell: inverse.
      const direction = token.delta > 0 && quoteDeltaUsd < 0 ? "BUY"
        : token.delta < 0 && quoteDeltaUsd > 0 ? "SELL" : null;
      if (!direction) continue;
      const candidate = {
        mint: token.mint,
        direction,
        tokenAmount: Math.abs(token.delta),
        quoteAmount: Math.abs(quoteDeltaUsd),
        quoteKind,
        trader: signer.pubkey,
        solDelta
      };
      if (!best || candidate.tokenAmount > best.tokenAmount) best = candidate;
    }
  }
  return best;
}

function getOrCreateCandidate(mint, dexName) {
  let p = state.pools.get(mint);
  if (!p) {
    if (state.pools.size >= CONFIG.maxCandidates) {
      const sorted = [...state.pools.values()].sort((a, b) => (a.lastSwapAt || 0) - (b.lastSwapAt || 0));
      const removable = sorted.find(x => !x.eligible);
      if (removable) state.pools.delete(removable.mint);
      else return null;
    }
    p = {
      address: mint,
      mint,
      poolAddress: null,
      poolProgramId: null,
      baseVault: null,
      quoteVault: null,
      quoteMint: null,
      liquidity: null,
      liquidityVerified: false,
      liquiditySol: null,
      quoteReserveSol: null,
      quoteLiquidity: null,
      buyVolumeSolEquivalent: 0,
      sellVolumeSolEquivalent: 0,
      volumeBuyPressure: null,
      name: "Token " + mint.slice(0, 6) + "…" + mint.slice(-4),
      symbol: mint.slice(0, 5).toUpperCase(),
      dex: dexName,
      liquidity: null,
      liquidityVerified: false,
      volume: 0,
      volumeUsd: null,
      volumeSol: 0,
      price: null,
      created: new Date().toISOString(),
      firstSeenAt: now(),
      lastSwapAt: 0,
      swaps: [],
      buyers: new Set(),
      buyCount: 0,
      sellCount: 0,
      buyVolumeSol: 0,
      sellVolumeSol: 0,
      buyVolumeUsd: 0,
      sellVolumeUsd: 0,
      buyPressure: 0,
      score: 0,
      eligible: false,
      risks: ["Analyse en cours"],
      security: { checked: false },
      quotes: { buyOk: false, sellOk: false },
      lastCheckAt: 0,
      checkError: null
    };
    state.pools.set(mint, p);
  }
  if (dexName && !String(p.dex).includes(dexName)) p.dex = String(p.dex).split(", ").concat(dexName).join(", ");
  return p;
}

function recalculateSwapMetrics(p) {
  p.buyers = new Set(p.swaps.filter(x => x.direction === "BUY").map(x => x.trader));
  p.buyCount = p.swaps.filter(x => x.direction === "BUY").length;
  p.sellCount = p.swaps.filter(x => x.direction === "SELL").length;
  p.buyVolumeSol = p.swaps.filter(x => x.direction === "BUY").reduce((s, x) => s + x.solAmount, 0);
  p.sellVolumeSol = p.swaps.filter(x => x.direction === "SELL").reduce((s, x) => s + x.solAmount, 0);
  p.buyVolumeUsd = p.swaps.filter(x => x.direction === "BUY").reduce((s, x) => s + (x.usdAmount || 0), 0);
  p.sellVolumeUsd = p.swaps.filter(x => x.direction === "SELL").reduce((s, x) => s + (x.usdAmount || 0), 0);
  p.buyVolumeSolEquivalent = p.swaps.filter(x => x.direction === "BUY" && x.solEquivalent != null).reduce((s, x) => s + x.solEquivalent, 0);
  p.sellVolumeSolEquivalent = p.swaps.filter(x => x.direction === "SELL" && x.solEquivalent != null).reduce((s, x) => s + x.solEquivalent, 0);
  const total = p.buyCount + p.sellCount;
  p.buyPressure = total ? p.buyCount / total : 0;
  const totalVolumeSol = p.buyVolumeSolEquivalent + p.sellVolumeSolEquivalent;
  p.volumeBuyPressure = totalVolumeSol > 0 ? p.buyVolumeSolEquivalent / totalVolumeSol : null;
  p.volumeSol = p.buyVolumeSol + p.sellVolumeSol;
  p.volumeUsd = p.buyVolumeUsd + p.sellVolumeUsd || null;
  p.volume = p.volumeUsd || p.volumeSol;
}

function addSwap(mint, swap, dexName, signature, slot, market) {
  const p = getOrCreateCandidate(mint, dexName);
  if (!p) return;
  const ts = now();
  const solAmount = swap.quoteKind === "SOL" ? swap.quoteAmount : 0;
  const usdAmount = swap.quoteKind === "USD_STABLE" ? swap.quoteAmount : null;
  const solEquivalent = swap.quoteKind === "SOL"
    ? swap.quoteAmount
    : (state.jupiter.lastSolPriceUsd > 0 ? swap.quoteAmount / state.jupiter.lastSolPriceUsd : null);
  p.swaps.push({ ts, direction: swap.direction, trader: swap.trader, solAmount, usdAmount, solEquivalent, signature, slot });
  p.swaps = p.swaps.filter(x => ts - x.ts <= CONFIG.windowMs);
  if (market) {
    p.poolAddress = market.poolAddress;
    p.poolProgramId = market.poolProgramId;
    p.baseVault = market.baseVault;
    p.quoteVault = market.quoteVault;
    p.quoteMint = market.quoteMint;
    p.liquidityVerified = false;
    p.liquidity = null;
    p.liquiditySol = null;
    p.quoteReserveSol = null;
    p.quoteLiquidity = null;
  }
  p.lastSwapAt = ts;
  recalculateSwapMetrics(p);
  state.lastUpdate = new Date().toISOString();
  state.helius.swapsDecoded++;
}

async function enrichCandidate(p) {
  try {
    const asset = await heliusRpc("getAsset", { id: p.mint });
    const metadata = asset?.content?.metadata || {};
    if (metadata.name) p.name = String(metadata.name).slice(0, 80);
    if (metadata.symbol) p.symbol = String(metadata.symbol).slice(0, 20);
    if (asset?.token_info?.decimals != null) p.decimals = Number(asset.token_info.decimals);
  } catch (_) {
    // Metadata is optional; the mint remains the reliable identifier.
  }
}

async function inspectMint(mint) {
  const info = await heliusRpc("getAccountInfo", [mint, { encoding: "jsonParsed", commitment: "confirmed" }]);
  const value = info && info.value;
  if (!value || !value.owner) throw new Error("Compte mint introuvable");
  const owner = value.owner;
  const parsed = value.data && value.data.parsed && value.data.parsed.info;
  if (!parsed) throw new Error("Données mint non décodées");
  const risks = [];
  if (owner !== TOKEN_PROGRAM && owner !== TOKEN_2022_PROGRAM) risks.push("Programme de token non reconnu");
  if (parsed.isInitialized === false) risks.push("Mint non initialisé");
  if (parsed.mintAuthority) risks.push("Mint authority encore active");
  if (parsed.freezeAuthority) risks.push("Freeze authority encore active");
  // Do not claim to inspect Token-2022 extensions from jsonParsed mint data.
  // For safety, Token-2022 mints are excluded until every extension is explicitly decoded.
  if (owner === TOKEN_2022_PROGRAM) risks.push("Token-2022 bloqué : extensions non vérifiées par ce scanner");
  return {
    checked: true,
    owner,
    tokenProgram: owner === TOKEN_PROGRAM ? "SPL_TOKEN" : owner === TOKEN_2022_PROGRAM ? "TOKEN_2022" : "UNKNOWN",
    decimals: Number(parsed.decimals),
    supply: String(parsed.supply || "0"),
    mintAuthority: parsed.mintAuthority || null,
    freezeAuthority: parsed.freezeAuthority || null,
    risks
  };
}

async function refreshSolPrice() {
  if (state.jupiter.lastSolPriceUsd > 0 && now() - state.jupiter.lastSolPriceAt < 5 * 60 * 1000) return state.jupiter.lastSolPriceUsd;
  const quote = await jupiterOrder(SOL_MINT, USDC_MINT, "1000000000");
  const price = Number(quote.outAmount) / 1e6;
  if (!Number.isFinite(price) || price <= 0) throw new Error("Prix SOL/USDC invalide");
  state.jupiter.lastSolPriceUsd = price;
  state.jupiter.lastSolPriceAt = now();
  state.jupiter.lastCheck = new Date().toISOString();
  return price;
}

// Jupiter Free allows 1 request/second over a 60-second sliding window.
// CART3R deliberately stays below that ceiling and shares ONE queue across
// every Jupiter caller (scanner, SOL price refresh and manual test endpoint).
const JUPITER_MIN_INTERVAL_MS = 1500;
let jupiterRequestTail = Promise.resolve();
let jupiterNextRequestAt = 0;
let jupiterCooldownUntil = 0;
let jupiterQueueDepth = 0;

function queueJupiterRequest(task) {
  jupiterQueueDepth++;
  const run = jupiterRequestTail.then(async () => {
    try {
      const waitUntil = Math.max(jupiterNextRequestAt, jupiterCooldownUntil);
      const waitMs = waitUntil - Date.now();
      if (waitMs > 0) await sleep(waitMs);

      jupiterNextRequestAt = Date.now() + JUPITER_MIN_INTERVAL_MS;
      state.jupiter.lastRequestAt = new Date().toISOString();
      state.jupiter.checksCount++;

      const result = await task();
      state.jupiter.successCount++;

      const rate = result && result.__rateLimit;
      if (rate && rate.remaining === 0 && rate.resetAt > Date.now()) {
        jupiterCooldownUntil = Math.max(jupiterCooldownUntil, rate.resetAt);
      }
      return result;
    } catch (e) {
      if (e && e.status === 429) {
        state.jupiter.rateLimitHits++;
        const resetAt = Number(e.rateLimitResetAt) || 0;
        const retryAfterAt = e.retryAfterMs > 0 ? Date.now() + e.retryAfterMs : 0;
        // Wait for the exact sliding-window reset (plus a small safety margin).
        // Only use 65 seconds when Jupiter omits both reset and Retry-After.
        const recoveryAt = resetAt > Date.now()
          ? resetAt + 250
          : retryAfterAt > Date.now()
            ? retryAfterAt + 250
            : Date.now() + 65000;
        jupiterCooldownUntil = Math.max(jupiterCooldownUntil, recoveryAt);
        state.jupiter.lastError = safeError(e);
      }
      throw e;
    } finally {
      jupiterQueueDepth = Math.max(0, jupiterQueueDepth - 1);
      state.jupiter.cooldownSeconds = Math.max(0, Math.ceil((jupiterCooldownUntil - Date.now()) / 1000));
    }
  });
  jupiterRequestTail = run.catch(() => undefined);
  return run;
}

async function jupiterOrder(inputMint, outputMint, amount) {
  if (!JUPITER_API_KEY) throw new Error("JUPITER_API_KEY non configurée");
  const url = new URL("https://api.jup.ag/swap/v2/order");
  url.searchParams.set("inputMint", inputMint);
  url.searchParams.set("outputMint", outputMint);
  url.searchParams.set("amount", String(amount));
  return queueJupiterRequest(async () => {
    const json = await fetchJson(url.toString(), {
      method: "GET",
      headers: { "accept": "application/json", "x-api-key": JUPITER_API_KEY }
    }, CONFIG.jupiterTimeoutMs);
    if (!json || !json.outAmount || Number(json.outAmount) <= 0) throw new Error("Jupiter ne retourne aucun montant de sortie");
    return json;
  });
}

async function verifyPoolLiquidity(p) {
  if (!p.poolAddress || p.poolProgramId !== RAYDIUM_CPMM_PROGRAM || !p.baseVault || !p.quoteVault) {
    p.liquidityVerified = false;
    return { ok: false, reason: "Pool Raydium CPMM et ses coffres non identifiés" };
  }
  const result = await heliusRpc("getMultipleAccounts", [
    [p.poolAddress, p.baseVault, p.quoteVault],
    { encoding: "jsonParsed", commitment: "confirmed" }
  ]);
  const accounts = result?.value || [];
  if (accounts.length !== 3 || accounts.some(x => !x)) throw new Error("Compte pool ou coffre introuvable");
  const [pool, baseVault, quoteVault] = accounts;
  if (pool.owner !== RAYDIUM_CPMM_PROGRAM) throw new Error("Propriétaire du pool CPMM invalide");
  const baseInfo = baseVault.data?.parsed?.info;
  const quoteInfo = quoteVault.data?.parsed?.info;
  if (!baseInfo || !quoteInfo || baseInfo.mint !== p.mint || quoteInfo.mint !== p.quoteMint) {
    throw new Error("Les coffres ne correspondent pas aux mints attendus");
  }
  const baseAmount = Number(baseInfo.tokenAmount?.uiAmountString ?? baseInfo.tokenAmount?.uiAmount ?? 0);
  const quoteAmount = Number(quoteInfo.tokenAmount?.uiAmountString ?? quoteInfo.tokenAmount?.uiAmount ?? 0);
  if (!Number.isFinite(baseAmount) || !Number.isFinite(quoteAmount) || baseAmount <= 0 || quoteAmount <= 0) {
    throw new Error("Réserves du pool nulles ou invalides");
  }
  let quoteSol = null;
  if (p.quoteMint === SOL_MINT) quoteSol = quoteAmount;
  else if (p.quoteMint === USDC_MINT || p.quoteMint === USDT_MINT) {
    const solPrice = await refreshSolPrice();
    quoteSol = quoteAmount / solPrice;
  }
  if (!Number.isFinite(quoteSol) || quoteSol <= 0) throw new Error("Réserve de cotation non convertible en SOL");
  p.quoteLiquidity = quoteAmount;
  p.quoteReserveSol = round(quoteSol, 4);
  // CPMM reserves are approximately value-balanced at spot: report an estimate, not an exact TVL.
  p.liquiditySol = round(quoteSol * 2, 4);
  p.liquidity = p.liquiditySol;
  p.liquidityVerified = quoteSol >= CONFIG.minLiquiditySol;
  p.liquidityCheckedAt = new Date().toISOString();
  p.baseReserve = round(baseAmount, 4);
  return {
    ok: p.liquidityVerified,
    reason: p.liquidityVerified ? null : "Liquidité de cotation inférieure à " + CONFIG.minLiquiditySol + " SOL"
  };
}

async function testRoundTrip(inputLamports, mint) {
  const buy = await jupiterOrder(SOL_MINT, mint, inputLamports);
  const tokenOut = String(buy.outAmount);
  const sell = await jupiterOrder(mint, SOL_MINT, tokenOut);
  const solIn = Number(inputLamports);
  const solBack = Number(sell.outAmount);
  const loss = solIn > 0 ? (1 - solBack / solIn) * 100 : null;
  if (!Number.isFinite(loss) || solBack <= 0) throw new Error("Retour SOL invalide");
  return {
    buyOk: true, sellOk: true,
    inputSol: solIn / 1e9, tokenOut, solBack: solBack / 1e9,
    roundTripLossPct: round(loss, 2),
    buyPriceImpactPct: buy.priceImpactPct == null ? null : toNum(buy.priceImpactPct),
    sellPriceImpactPct: sell.priceImpactPct == null ? null : toNum(sell.priceImpactPct)
  };
}

async function checkCandidate(p) {
  if (!p || !state.running) return;
  p.lastCheckAt = now();
  p.checkError = null;
  const risks = [];
  try {
    p.security = await inspectMint(p.mint);
    risks.push(...p.security.risks);
  } catch (e) {
    p.security = { checked: false, error: safeError(e), risks: [] };
    risks.push("Sécurité mint non vérifiée");
  }

  if (JUPITER_API_KEY) {
    try {
      const solPrice = await refreshSolPrice();
      for (const swap of p.swaps) {
        swap.solEquivalent = swap.usdAmount != null ? swap.usdAmount / solPrice : swap.solAmount;
      }
      recalculateSwapMetrics(p);
    } catch (_) {
      // Keep stablecoin-denominated flow unverified if the SOL/USD reference is unavailable.
    }
  }

  try {
    const market = await verifyPoolLiquidity(p);
    if (!market.ok) risks.push(market.reason);
  } catch (e) {
    p.liquidityVerified = false;
    p.liquidity = null;
    p.liquiditySol = null;
    p.quoteReserveSol = null;
    risks.push("Liquidité du pool non vérifiée : " + safeError(e));
  }

  let small = null, medium = null, roundTripLossPct = null;
  if (p.security.checked && p.security.risks.length === 0 && JUPITER_API_KEY) {
    try {
      state.jupiter.lastCheck = new Date().toISOString();
      small = await testRoundTrip(CONFIG.quoteLadderLamports[0], p.mint);
      medium = await testRoundTrip(CONFIG.quoteLadderLamports[1], p.mint);
      roundTripLossPct = small.roundTripLossPct;
      p.quotes = {
        buyOk: small.buyOk && medium.buyOk,
        sellOk: small.sellOk && medium.sellOk,
        testInputSol: small.inputSol,
        tokenOut: small.tokenOut,
        solBack: small.solBack,
        roundTripLossPct: small.roundTripLossPct,
        buyPriceImpactPct: small.buyPriceImpactPct,
        sellPriceImpactPct: small.sellPriceImpactPct,
        quoteLadder: [small, medium],
        checkedAt: new Date().toISOString()
      };
      state.jupiter.lastError = null;
      if (small.roundTripLossPct > CONFIG.maxRoundTripLossPct || medium.roundTripLossPct > CONFIG.maxRoundTripLossPct) {
        risks.push("Coût aller-retour supérieur à " + CONFIG.maxRoundTripLossPct + "% sur la grille de cotations");
      }
    } catch (e) {
      p.quotes = { buyOk: Boolean(small?.buyOk || medium?.buyOk), sellOk: Boolean(small?.sellOk || medium?.sellOk), quoteLadder: [small, medium].filter(Boolean), error: safeError(e), checkedAt: new Date().toISOString() };
      state.jupiter.lastError = safeError(e);
      risks.push("Grille de cotations achat/revente incomplète");
    }
  } else if (!JUPITER_API_KEY) {
    risks.push("Clé Jupiter absente : test achat/revente impossible");
  } else {
    risks.push("Sécurité mint non validée");
  }

  const total = p.buyCount + p.sellCount;
  const buyers = p.buyers.size;
  const pressure = total ? p.buyCount / total : 0;
  p.buyPressure = pressure;
  if (total < CONFIG.minSwaps) risks.push("Pas assez de swaps observés (" + total + "/" + CONFIG.minSwaps + ")");
  if (buyers < CONFIG.minBuyers) risks.push("Pas assez d'acheteurs distincts (" + buyers + "/" + CONFIG.minBuyers + ")");
  if (pressure < CONFIG.minBuyPressure) risks.push("Pression acheteuse insuffisante en nombre de swaps");
  if (p.volumeBuyPressure == null) risks.push("Pression acheteuse en volume non vérifiable");
  else if (p.volumeBuyPressure < CONFIG.minBuyPressure) risks.push("Pression acheteuse insuffisante en volume");

  const securityScore = p.security.checked && p.security.risks.length === 0 ? 25 : 0;
  const activityScore = Math.min(15, total * 1.5);
  const buyerScore = Math.min(15, buyers * 3);
  const pressureScore = Math.round(15 * pressure);
  const volumeScore = p.volumeBuyPressure == null ? 0 : Math.round(15 * p.volumeBuyPressure);
  const liquidityScore = p.liquidityVerified ? 10 : 0;
  const quoteScore = p.quotes.buyOk && p.quotes.sellOk && roundTripLossPct != null && roundTripLossPct <= CONFIG.maxRoundTripLossPct ? 5 : 0;
  p.score = Math.round(securityScore + activityScore + buyerScore + pressureScore + volumeScore + liquidityScore + quoteScore);
  p.eligible = risks.length === 0 && Boolean(p.poolAddress) && p.liquidityVerified &&
    p.quotes.buyOk && p.quotes.sellOk && small && medium &&
    small.roundTripLossPct <= CONFIG.maxRoundTripLossPct &&
    medium.roundTripLossPct <= CONFIG.maxRoundTripLossPct;
  p.risks = [...new Set(risks)];
  p.roundTripLossPct = roundTripLossPct == null ? null : round(roundTripLossPct, 2);
  p.buyersCount = buyers;
  p.activityCount = total;
  p.checkedAt = new Date().toISOString();
}

async function runChecks() {
  if (!state.running || state.scanning) return;
  state.scanning = true;
  try {
    const candidates = [...state.pools.values()]
      .filter(p => now() - p.lastCheckAt >= CONFIG.checkEveryMs)
      .sort((a, b) => (b.swaps.length - a.swaps.length) || (b.lastSwapAt - a.lastSwapAt))
      .slice(0, CONFIG.checkBatch);
    for (const p of candidates) {
      if (!state.running) break;
      await checkCandidate(p);
      await sleep(250);
    }
    state.lastUpdate = new Date().toISOString();
  } catch (e) {
    state.lastError = safeError(e);
  } finally {
    state.scanning = false;
  }
}

function rememberSignature(signature) {
  if (!signature || state.seenSignatures.has(signature)) return false;
  state.seenSignatures.add(signature);
  if (state.seenSignatures.size > CONFIG.maxSeenSignatures) {
    const first = state.seenSignatures.values().next().value;
    state.seenSignatures.delete(first);
  }
  return true;
}

async function processSignature(signature, dexName, slot) {
  try {
    const tx = await heliusRpc("getTransaction", [signature, {
      encoding: "jsonParsed",
      commitment: "confirmed",
      maxSupportedTransactionVersion: 1
    }]);
    state.helius.lastCheck = new Date().toISOString();
    state.helius.lastSlot = slot || tx?.slot || state.helius.lastSlot;
    if (!tx || !tx.meta || tx.meta.err) return;
    const swap = findSwapForSigner(tx);
    if (!swap || !swap.mint || swap.mint === SOL_MINT || swap.mint === USDC_MINT || swap.mint === USDT_MINT) return;
    const market = extractRaydiumCpmmMarket(tx, swap.mint);
    addSwap(swap.mint, swap, dexName, signature, slot || tx.slot, market);
    const p = state.pools.get(swap.mint);
    if (p && !p.metadataRequested) {
      p.metadataRequested = true;
      enrichCandidate(p).catch(() => {});
    }
    state.discovery.successCount++;
    state.discovery.lastCheck = new Date().toISOString();
    state.scanner = "LIVE";
  } catch (e) {
    state.helius.lastError = safeError(e);
    state.discovery.failureCount++;
    state.discovery.lastError = safeError(e);
  }
}

async function drainQueue() {
  while (state.running && state.processing < CONFIG.maxConcurrentTransactions && state.signatureQueue.length) {
    const item = state.signatureQueue.shift();
    state.processing++;
    processSignature(item.signature, item.dexName, item.slot)
      .catch(e => { state.helius.lastError = safeError(e); })
      .finally(() => {
        state.processing--;
        drainQueue();
      });
  }
}

function startHeliusSocket() {
  if (!state.running || !HELIUS_RPC_URL) {
    state.scanner = HELIUS_RPC_URL ? "STOPPED" : "ERROR";
    if (!HELIUS_RPC_URL) state.lastError = "HELIUS_RPC_URL non configurée";
    return;
  }
  const WebSocketImpl = globalThis.WebSocket;
  if (!WebSocketImpl) {
    state.scanner = "ERROR";
    state.lastError = "WebSocket natif absent : configure Node.js 22 ou supérieur sur Render";
    return;
  }
  const url = rpcWebSocketUrl();
  if (!url) {
    state.scanner = "ERROR";
    state.lastError = "URL WebSocket Helius invalide";
    return;
  }
  try {
    state.scanner = "CONNECTING";
    const ws = new WebSocketImpl(url);
    state.ws = ws;
    let requestId = 1;
    const pending = new Map();
    const subscriptions = new Map();

    ws.addEventListener("open", () => {
      state.helius.connected = true;
      state.helius.lastError = null;
      state.scanner = "LIVE";
      for (const program of DEX_PROGRAMS) {
        const id = requestId++;
        pending.set(id, program);
        ws.send(JSON.stringify({
          jsonrpc: "2.0",
          id,
          method: "logsSubscribe",
          params: [{ mentions: [program.id] }, { commitment: "confirmed" }]
        }));
      }
      state.helius.subscriptions = DEX_PROGRAMS.length;
      state.discovery.callsCount += DEX_PROGRAMS.length;
      state.discovery.lastCheck = new Date().toISOString();
    });

    ws.addEventListener("message", event => {
      let msg;
      try { msg = JSON.parse(String(event.data)); } catch (_) { return; }
      if (msg.id && pending.has(msg.id)) {
        const program = pending.get(msg.id);
        pending.delete(msg.id);
        if (msg.error) {
          state.helius.lastError = msg.error.message || "Échec abonnement Helius";
          state.discovery.failureCount++;
        } else {
          subscriptions.set(msg.result, program);
          state.helius.subscriptions = subscriptions.size;
        }
        return;
      }
      if (msg.method !== "logsNotification") return;
      const result = msg.params?.result;
      const value = result?.value;
      const signature = value?.signature;
      if (!signature || value.err) return;
      const logs = Array.isArray(value.logs) ? value.logs : [];
      // Keep only swap-like transactions; pool setup/admin traffic is not a trading signal.
      // Accept explicit swap instruction logs, not any incidental mention of the word "swap".
      if (!logs.some(line => /Program log: Instruction:.*\bswap[a-z0-9_]*\b/i.test(String(line)))) return;
      if (!rememberSignature(signature)) return;
      const subId = result?.subscription;
      const program = subscriptions.get(subId);
      state.helius.transactionsSeen++;
      // Keep the newest events if the RPC backlog grows beyond a safe bound.
      if (state.signatureQueue.length >= CONFIG.maxQueuedSignatures) {
        state.signatureQueue.shift();
        state.helius.droppedQueuedTransactions++;
      }
      state.signatureQueue.push({ signature, dexName: program?.name || "DEX Solana", slot: result?.context?.slot || null });
      drainQueue();
    });

    ws.addEventListener("error", () => {
      state.helius.lastError = "Erreur WebSocket Helius";
      state.scanner = "DEGRADED";
    });

    ws.addEventListener("close", () => {
      state.helius.connected = false;
      state.helius.subscriptions = 0;
      if (!state.running) return;
      state.scanner = "DEGRADED";
      state.helius.reconnects++;
      clearTimeout(state.wsReconnectTimer);
      state.wsReconnectTimer = setTimeout(startHeliusSocket, Math.min(30000, 3000 * Math.max(1, state.helius.reconnects)));
    });
  } catch (e) {
    state.helius.connected = false;
    state.scanner = "DEGRADED";
    state.helius.lastError = safeError(e);
    clearTimeout(state.wsReconnectTimer);
    state.wsReconnectTimer = setTimeout(startHeliusSocket, 10000);
  }
}

function publicCandidate(p) {
  const swaps = p.swaps || [];
  return {
    address: p.mint,
    mint: p.mint,
    poolAddress: p.poolAddress,
    name: p.name,
    symbol: p.symbol,
    dex: p.dex,
    liquidity: p.liquidity,
    liquidityVerified: Boolean(p.liquidityVerified),
    liquiditySol: p.liquiditySol,
    estimatedLiquiditySol: p.liquiditySol,
    quoteReserveSol: p.quoteReserveSol,
    quoteLiquidity: p.quoteLiquidity,
    quoteMint: p.quoteMint,
    volume: p.volume,
    volumeUsd: p.volumeUsd,
    volumeSol: round(p.volumeSol, 6),
    price: null,
    created: p.created,
    firstSeenAt: new Date(p.firstSeenAt).toISOString(),
    lastSwapAt: p.lastSwapAt ? new Date(p.lastSwapAt).toISOString() : null,
    activityWindowMinutes: 15,
    activityCount: swaps.length,
    buyCount: p.buyCount,
    sellCount: p.sellCount,
    buyersCount: p.buyers.size,
    buyPressure: round(p.buyPressure * 100, 1),
    buyPressureByVolume: p.volumeBuyPressure == null ? null : round(p.volumeBuyPressure * 100, 1),
    buyVolumeSol: round(p.buyVolumeSol, 6),
    sellVolumeSol: round(p.sellVolumeSol, 6),
    score: p.score,
    eligible: p.eligible,
    risks: p.risks,
    security: p.security,
    quotes: p.quotes,
    roundTripLossPct: p.roundTripLossPct,
    checkedAt: p.checkedAt || null
  };
}

function statusPayload() {
  const pools = [...state.pools.values()];
  const eligible = pools.filter(p => p.eligible);
  return {
    running: state.running,
    mode: "TEST",
    realTrading: false,
    scanner: state.scanner,
    poolsFound: pools.length,
    catalogPools: pools.length,
    eligiblePools: eligible.length,
    lastUpdate: state.lastUpdate,
    lastError: state.lastError,
    scanning: state.scanning,
    nextScanInSeconds: state.running ? Math.max(0, Math.ceil(CONFIG.checkEveryMs / 1000)) : 0,
    startedAt: state.startedAt,
    discovery: { ...state.discovery, cachedFeeds: pools.length },
    helius: { ...state.helius, configured: Boolean(HELIUS_RPC_URL) },
    jupiter: {
      ...state.jupiter,
      configured: Boolean(JUPITER_API_KEY),
      cooldownSeconds: Math.max(0, Math.ceil((jupiterCooldownUntil - Date.now()) / 1000)),
      minIntervalMs: JUPITER_MIN_INTERVAL_MS,
      queueDepth: jupiterQueueDepth
    },
    protections: { ...state.protections }
  };
}

app.get("/", (_req, res) => res.json({
  name: "CART3R.JAILBREAK",
  version: "HELIUS-ONCHAIN-1",
  mode: "TEST",
  realTrading: false,
  message: "Scanner Helius en lecture seule. Les swaps réels sont assemblés par Jupiter et nécessitent une signature Phantom explicite.",
  endpoints: ["/health", "/status", "/pools", "/wallet", "/trade/quote", "/trade/order", "/trade/execute", "/security", "/helius-test", "/jupiter-test", "/gecko-test", "/test/start", "/test/stop"]
}));

app.get("/health", (_req, res) => res.json({ ok: true, service: "CART3R", mode: "TEST", realTrading: false }));

const tradeOrders = new Map();
const tradeRate = new Map();
function tradeLimit(req, res, next) {
  const key = req.ip || req.socket?.remoteAddress || "unknown";
  const t = Date.now(), prior = tradeRate.get(key) || [];
  const recent = prior.filter(x => t - x < 10000);
  if (recent.length >= 12) return res.status(429).json({ ok:false, error:"Trop de requêtes trading. Réessaie dans quelques secondes." });
  recent.push(t); tradeRate.set(key, recent); next();
}
const validBase58 = value => typeof value === "string" && /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value);
async function jupiterRequest(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (JUPITER_API_KEY) headers["x-api-key"] = JUPITER_API_KEY;
  const response = await fetch("https://api.jup.ag/swap/v2" + path, { ...options, headers });
  const body = await response.text();
  let json = {};
  try { json = body ? JSON.parse(body) : {}; } catch (_) {}
  if (!response.ok) throw new Error("Jupiter HTTP " + response.status + ": " + (json.error || json.message || body.slice(0,180)));
  return json;
}
app.get("/wallet", tradeLimit, async (req, res) => {
  const address = String(req.query.address || "");
  if (!validBase58(address)) return res.status(400).json({ok:false,error:"Adresse wallet invalide"});
  try {
    const [lamports, legacyAccounts, token2022Accounts] = await Promise.all([
      heliusRpc("getBalance", [address, { commitment:"confirmed" }]),
      heliusRpc("getTokenAccountsByOwner", [address, { programId:TOKEN_PROGRAM }, { encoding:"jsonParsed", commitment:"confirmed" }]),
      heliusRpc("getTokenAccountsByOwner", [address, { programId:TOKEN_2022_PROGRAM }, { encoding:"jsonParsed", commitment:"confirmed" }])
    ]);
    const tokens = [...(legacyAccounts?.value || []), ...(token2022Accounts?.value || [])].map(row => {
      const info = row.account?.data?.parsed?.info;
      const amount = info?.tokenAmount || {};
      return { mint:info?.mint, amount:amount.amount || "0", uiAmount:amount.uiAmountString || amount.uiAmount || "0", decimals:Number(amount.decimals || 0), tokenAccount:row.pubkey };
    }).filter(x => x.mint && BigInt(x.amount || "0") > 0n);
    res.json({ok:true,address,sol:Number(lamports?.value || 0)/1e9,lamports:String(lamports?.value || 0),tokens,checkedAt:new Date().toISOString()});
  } catch(e) { res.status(502).json({ok:false,error:safeError(e)}); }
});
app.get("/trade/quote", tradeLimit, async (req,res) => {
  const inputMint=String(req.query.inputMint||""), outputMint=String(req.query.outputMint||""), amount=String(req.query.amount||"");
  if (!validBase58(inputMint) || !validBase58(outputMint) || inputMint===outputMint || !/^[1-9][0-9]{0,19}$/.test(amount))
    return res.status(400).json({ok:false,error:"Paramètres de cotation invalides"});
  try {
    const q=new URLSearchParams({inputMint,outputMint,amount});
    const order=await jupiterRequest("/order?"+q.toString());
    res.json({ok:true,quote:order,checkedAt:new Date().toISOString()});
  } catch(e) { res.status(502).json({ok:false,error:safeError(e)}); }
});
app.post("/trade/order", tradeLimit, async (req,res) => {
  const inputMint=String(req.body?.inputMint||""), outputMint=String(req.body?.outputMint||"");
  const amount=String(req.body?.amount||""), taker=String(req.body?.taker||"");
  if (!validBase58(inputMint) || !validBase58(outputMint) || inputMint===outputMint || !validBase58(taker) || !/^[1-9][0-9]{0,19}$/.test(amount))
    return res.status(400).json({ok:false,error:"Paramètres d'ordre invalides"});
  try {
    const q=new URLSearchParams({inputMint,outputMint,amount,taker,maxSupportedTransactionVersion:"0"});
    const order=await jupiterRequest("/order?"+q.toString());
    if (!order.transaction || !order.requestId) return res.status(422).json({ok:false,error:order.errorMessage||"Jupiter n'a pas construit de transaction signable",router:order.router});
    tradeOrders.set(order.requestId,{taker,createdAt:Date.now(),order});
    for (const [id,item] of tradeOrders) if (Date.now()-item.createdAt>120000) tradeOrders.delete(id);
    res.json({ok:true,order});
  } catch(e) { res.status(502).json({ok:false,error:safeError(e)}); }
});
app.post("/trade/execute", tradeLimit, async (req,res) => {
  const requestId=String(req.body?.requestId||""), signedTransaction=String(req.body?.signedTransaction||"");
  const cached=tradeOrders.get(requestId);
  if (!cached || Date.now()-cached.createdAt>120000) return res.status(410).json({ok:false,error:"Ordre expiré ou inconnu. Demande une nouvelle cotation."});
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(signedTransaction) || signedTransaction.length>20000)
    return res.status(400).json({ok:false,error:"Transaction signée invalide"});
  tradeOrders.delete(requestId);
  try {
    const result=await jupiterRequest("/execute",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({signedTransaction,requestId})});
    res.json({ok:true,result});
  } catch(e) { res.status(502).json({ok:false,error:safeError(e)}); }
});

app.get("/status", (_req, res) => res.json(statusPayload()));
app.get("/pools", (_req, res) => {
  const list = [...state.pools.values()].map(publicCandidate)
    .sort((a, b) => Number(b.eligible) - Number(a.eligible) || b.score - a.score || b.activityCount - a.activityCount)
    .slice(0, CONFIG.maxCandidates);
  res.json({ ok: true, mode: "TEST", realTrading: false, count: list.length, pools: list });
});
app.get("/security", async (req, res) => {
  const mint = String(req.query.address || req.query.mint || "").trim();
  if (!mint) return res.status(400).json({ ok: false, error: "Paramètre address ou mint manquant" });
  try {
    const security = await inspectMint(mint);
    res.json({ ok: true, mint, security, safe: security.risks.length === 0, realTrading: false });
  } catch (e) {
    res.status(502).json({ ok: false, mint, error: safeError(e), safe: false });
  }
});
app.get("/helius-test", async (_req, res) => {
  try {
    const slot = await heliusRpc("getSlot", [{ commitment: "confirmed" }]);
    state.helius.lastCheck = new Date().toISOString();
    state.helius.lastSlot = slot;
    state.helius.lastError = null;
    res.json({ ok: true, slot, checkedAt: state.helius.lastCheck, websocketConnected: state.helius.connected });
  } catch (e) {
    state.helius.lastError = safeError(e);
    res.status(502).json({ ok: false, error: safeError(e) });
  }
});
app.get("/jupiter-test", async (_req, res) => {
  try {
    const quote = await jupiterOrder(SOL_MINT, USDC_MINT, "1000000000");
    state.jupiter.lastCheck = new Date().toISOString();
    state.jupiter.lastSolPriceUsd = Number(quote.outAmount) / 1e6;
    state.jupiter.lastSolPriceAt = now();
    state.jupiter.lastError = null;
    res.json({ ok: true, api: "SWAP_V2_ORDER", inputSol: 1, outputUsdc: state.jupiter.lastSolPriceUsd, checkedAt: state.jupiter.lastCheck, quoteOnly: true });
  } catch (e) {
    state.jupiter.lastError = safeError(e);
    res.status(502).json({ ok: false, error: safeError(e), quoteOnly: true });
  }
});
app.get("/gecko-test", (_req, res) => res.json({
  ok: true,
  disabled: true,
  provider: "HELIUS",
  message: "GeckoTerminal a été retiré du scanner pour éviter les erreurs HTTP 429."
}));
app.post("/test/start", (_req, res) => {
  if (!state.running) {
    state.running = true;
    state.scanner = "STARTING";
    startHeliusSocket();
  }
  res.json({ ok: true, running: state.running, mode: "TEST", realTrading: false });
});
app.post("/test/stop", (_req, res) => {
  state.running = false;
  state.scanner = "STOPPED";
  clearTimeout(state.wsReconnectTimer);
  clearInterval(state.checkTimer);
  if (state.ws) {
    try { state.ws.close(); } catch (_) {}
    state.ws = null;
  }
  state.helius.connected = false;
  res.json({ ok: true, running: false, mode: "TEST", realTrading: false });
});


/* CART3R AUTO PAPER TRADER — server-side simulation only. */
const fs = require("fs");
const path = require("path");
const PAPER_FILE = process.env.PAPER_STATE_FILE || "/var/data/cart3r-paper.json";
const AUTO_CONTROL_TOKEN = String(process.env.AUTO_CONTROL_TOKEN || "");
const PAPER_DEFAULTS = { enabled:false, initialSol:10, cashSol:10, orderSol:0.05, stopLossPct:10, takeProfitPct:25, minScore:75, maxPositions:3, maxDailyLossSol:0.5, scanEveryMs:45000 };
const paper = { config:{...PAPER_DEFAULTS}, positions:[], history:[], news:[], lastNewsAt:0, lastTickAt:null, lastError:null, storage:"unknown", busy:false };
function savePaper() {
 const data=JSON.stringify({config:paper.config,positions:paper.positions,history:paper.history.slice(0,250),news:paper.news.slice(0,60),lastNewsAt:paper.lastNewsAt});
 try { fs.mkdirSync(path.dirname(PAPER_FILE),{recursive:true}); fs.writeFileSync(PAPER_FILE,data,"utf8"); paper.storage=PAPER_FILE; }
 catch(e) { try { const f=path.join(process.cwd(),"cart3r-paper-state.json"); fs.writeFileSync(f,data,"utf8"); paper.storage="EPHEMERAL:"+f; } catch(x) { paper.storage="ERROR"; paper.lastError="Sauvegarde impossible : "+safeError(x); } }
}
function loadPaper() {
 for(const f of [PAPER_FILE,path.join(process.cwd(),"cart3r-paper-state.json")]) try {
  if(!fs.existsSync(f)) continue; const d=JSON.parse(fs.readFileSync(f,"utf8"));
  if(d.config) paper.config={...PAPER_DEFAULTS,...d.config};
  if(Array.isArray(d.positions)) paper.positions=d.positions;
  if(Array.isArray(d.history)) paper.history=d.history.slice(0,250);
  if(Array.isArray(d.news)) paper.news=d.news.slice(0,60);
  paper.lastNewsAt=Number(d.lastNewsAt)||0; paper.storage=f; return;
 } catch(e) { paper.lastError="État simulation illisible : "+safeError(e); }
}
loadPaper();
function decodeFeedText(v) { return String(v||"").replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g,"$1").replace(/<[^>]*>/g," ").replace(/&amp;/g,"&").replace(/&quot;/g,'"').replace(/&#39;|&apos;/g,"'").replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/\s+/g," ").trim().slice(0,900); }
function feedTag(block,tag) { const m=block.match(new RegExp("<"+tag+"(?:\\s[^>]*)?>([\\s\\S]*?)</"+tag+">","i")); return m?decodeFeedText(m[1]):""; }
function scoreNews(text) {
 const s=String(text||"").toLowerCase();
 const pos=["approval","approved","partnership","launch","integration","adoption","inflow","surge","upgrade","bullish","record high","listing","acquisition","funding","etf"];
 const neg=["hack","exploit","scam","lawsuit","ban","investigation","breach","stolen","rug pull","rugpull","outage","delist","bankruptcy","insolvency","fraud","drain","vulnerability"];
 let p=0,n=0; for(const w of pos)if(s.includes(w))p++; for(const w of neg)if(s.includes(w))n++;
 return Math.max(-1,Math.min(1,(p-n*1.5)/Math.max(2,p+n)));
}
async function refreshCryptoNews(force=false) {
 if(!force&&Date.now()-paper.lastNewsAt<300000)return paper.news;
 const feeds=[{name:"CoinDesk",url:"https://www.coindesk.com/arc/outboundfeeds/rss/"},{name:"Cointelegraph",url:"https://cointelegraph.com/rss"},{name:"Decrypt",url:"https://decrypt.co/feed"}];
 const all=[];
 await Promise.all(feeds.map(async f=>{let timer;try{
  const c=new AbortController();timer=setTimeout(()=>c.abort(),9000);
  const r=await fetch(f.url,{signal:c.signal,headers:{"user-agent":"CART3R-JAILBREAK/1.0"}});clearTimeout(timer);
  if(!r.ok)throw new Error("HTTP "+r.status);const xml=await r.text();const blocks=xml.match(/<(item|entry)\b[\s\S]*?<\/(item|entry)>/gi)||[];
  for(const b of blocks.slice(0,20)){const title=feedTag(b,"title"),description=feedTag(b,"description")||feedTag(b,"summary")||feedTag(b,"content");
   const link=feedTag(b,"link")||((b.match(/<link[^>]+href=["']([^"']+)/i)||[])[1]||"");
   const published=feedTag(b,"pubDate")||feedTag(b,"published")||feedTag(b,"updated");if(!title)continue;
   all.push({source:f.name,title,description,link,publishedAt:published||null,sentiment:scoreNews(title+" "+description),fetchedAt:new Date().toISOString()});
  }
 }catch(e){if(timer)clearTimeout(timer);}}));
 const unique=new Map();for(const n of all)unique.set(n.title.toLowerCase(),n);
 paper.news=[...unique.values()].sort((a,b)=>Date.parse(b.publishedAt||0)-Date.parse(a.publishedAt||0)).slice(0,60);
 paper.lastNewsAt=Date.now();savePaper();return paper.news;
}
function newsSignalFor(p) {
 const symbol=String(p.symbol||"").toLowerCase().replace(/[^a-z0-9]/g,""),name=String(p.name||"").toLowerCase();
 const matches=paper.news.filter(n=>{const h=(n.title+" "+n.description).toLowerCase();return(symbol.length>=3&&h.includes(symbol))||(name.length>=5&&h.includes(name));}).slice(0,8);
 return {sentiment:matches.length?Number((matches.reduce((s,n)=>s+n.sentiment,0)/matches.length).toFixed(2)):0,count:matches.length,articles:matches};
}
function paperDailyPnl(){const day=new Date().toISOString().slice(0,10);return paper.history.filter(x=>x.type==="AUTO_CLOSE"&&String(x.closedAt||"").slice(0,10)===day).reduce((s,x)=>s+Number(x.pnlSol||0),0);}
function paperPublicState(){return{ok:true,mode:"PAPER_ONLY",realTrading:false,controlConfigured:Boolean(AUTO_CONTROL_TOKEN),config:{...paper.config},active:Boolean(paper.config.enabled),positions:paper.positions,history:paper.history.slice(0,80),news:paper.news.slice(0,30),newsUpdatedAt:paper.lastNewsAt?new Date(paper.lastNewsAt).toISOString():null,lastTickAt:paper.lastTickAt,lastError:paper.lastError,dailyPnlSol:Number(paperDailyPnl().toFixed(6)),storage:paper.storage,running:true};}
function requireAutoControl(req,res,next){
 if(!AUTO_CONTROL_TOKEN)return res.status(503).json({ok:false,error:"AUTO_CONTROL_TOKEN doit être configuré dans Render avant de modifier le bot."});
 const supplied=String(req.get("x-cart3r-control")||"");
 if(supplied.length!==AUTO_CONTROL_TOKEN.length||!crypto.timingSafeEqual(Buffer.from(supplied),Buffer.from(AUTO_CONTROL_TOKEN)))return res.status(401).json({ok:false,error:"Code de contrôle incorrect."});
 next();
}
function validatePaperConfig(body){
 const next={...paper.config};const keys=["initialSol","orderSol","stopLossPct","takeProfitPct","minScore","maxPositions","maxDailyLossSol"];
 for(const k of keys)if(body[k]!=null){const n=Number(body[k]);if(!Number.isFinite(n))throw new Error("Valeur invalide : "+k);next[k]=n;}
 if(next.initialSol<.1||next.initialSol>100000)throw new Error("Capital fictif : 0,1 à 100000 SOL.");
 if(next.orderSol<.001||next.orderSol>next.initialSol)throw new Error("Montant par position invalide.");
 if(next.stopLossPct<.1||next.stopLossPct>99)throw new Error("Stop-loss : 0,1 à 99 %.");
 if(next.takeProfitPct<.1||next.takeProfitPct>1000)throw new Error("Take-profit : 0,1 à 1000 %.");
 if(next.minScore<0||next.minScore>100)throw new Error("Score minimum : 0 à 100.");
 if(!Number.isInteger(next.maxPositions)||next.maxPositions<1||next.maxPositions>20)throw new Error("Positions max : 1 à 20.");
 if(next.maxDailyLossSol<0||next.maxDailyLossSol>next.initialSol)throw new Error("Limite de perte journalière invalide.");
 return next;
}
async function closePaperPosition(p,q,reason){
 const proceeds=Number(q.outAmount)/1e9;if(!Number.isFinite(proceeds)||proceeds<=0)throw new Error("Cotation de sortie invalide.");
 const pnl=proceeds-Number(p.entrySol);paper.config.cashSol=Number((paper.config.cashSol+proceeds).toFixed(9));
 paper.history.unshift({type:"AUTO_CLOSE",reason,mint:p.mint,symbol:p.symbol,openedAt:p.openedAt,closedAt:new Date().toISOString(),entrySol:p.entrySol,proceedsSol:proceeds,pnlSol:Number(pnl.toFixed(9)),pnlPct:Number((pnl/p.entrySol*100).toFixed(2))});
 paper.positions=paper.positions.filter(x=>x.id!==p.id);savePaper();
}
async function runPaperTrader(){
 if(paper.busy)return;paper.busy=true;
 try{
  await refreshCryptoNews(false);
  for(const p of [...paper.positions])try{
   const q=await jupiterOrder(p.mint,SOL_MINT,String(p.rawAmount)),v=Number(q.outAmount)/1e9;
   if(!Number.isFinite(v)||v<=0)continue;p.currentValueSol=Number(v.toFixed(9));p.pnlPct=Number(((v/p.entrySol-1)*100).toFixed(2));p.lastQuoteAt=new Date().toISOString();p.quoteError=null;
   if(p.pnlPct<=-p.stopLossPct)await closePaperPosition(p,q,"STOP_LOSS");else if(p.pnlPct>=p.takeProfitPct)await closePaperPosition(p,q,"TAKE_PROFIT");else savePaper();
  }catch(e){p.quoteError=safeError(e);paper.lastError="Surveillance "+p.symbol+" : "+safeError(e);}
  if(paper.config.enabled&&paper.positions.length<paper.config.maxPositions&&paperDailyPnl()>-paper.config.maxDailyLossSol&&JUPITER_API_KEY){
   const candidates=[...state.pools.values()].filter(p=>p.eligible&&p.score>=paper.config.minScore).sort((a,b)=>b.score-a.score);
   for(const c of candidates){
    if(paper.positions.length>=paper.config.maxPositions)break;if(paper.positions.some(p=>p.mint===c.mint))continue;
    const signal=newsSignalFor(c);if(signal.sentiment<=-.25)continue;if(paper.config.cashSol<paper.config.orderSol)break;
    try{
     const q=await jupiterOrder(SOL_MINT,c.mint,String(Math.round(paper.config.orderSol*1e9)));if(!q.outAmount||Number(q.outAmount)<=0)continue;
     const p={id:c.mint+":"+Date.now(),mint:c.mint,name:c.name,symbol:c.symbol,rawAmount:String(q.outAmount),entrySol:paper.config.orderSol,currentValueSol:paper.config.orderSol,stopLossPct:paper.config.stopLossPct,takeProfitPct:paper.config.takeProfitPct,score:c.score,newsSentiment:signal.sentiment,newsMatches:signal.count,openedAt:new Date().toISOString(),lastQuoteAt:new Date().toISOString()};
     paper.config.cashSol=Number((paper.config.cashSol-paper.config.orderSol).toFixed(9));paper.positions.push(p);paper.history.unshift({type:"AUTO_OPEN",mint:p.mint,symbol:p.symbol,openedAt:p.openedAt,entrySol:p.entrySol,score:p.score,newsSentiment:p.newsSentiment});savePaper();
    }catch(e){paper.lastError="Ouverture "+c.symbol+" : "+safeError(e);}
   }
  }
  paper.lastTickAt=new Date().toISOString();
 }catch(e){paper.lastError=safeError(e);}finally{paper.busy=false;savePaper();}
}
app.get("/auto/status",(_req,res)=>res.json(paperPublicState()));
app.get("/auto/news",async(_req,res)=>{try{await refreshCryptoNews(false);res.json({ok:true,news:paper.news,updatedAt:paper.lastNewsAt?new Date(paper.lastNewsAt).toISOString():null});}catch(e){res.status(502).json({ok:false,error:safeError(e),news:paper.news});}});
app.post("/auto/config",requireAutoControl,(req,res)=>{try{const n=validatePaperConfig(req.body||{});if(paper.positions.length&&n.initialSol!==paper.config.initialSol)return res.status(409).json({ok:false,error:"Ferme les positions avant de modifier le capital initial."});if(!paper.positions.length&&n.initialSol!==paper.config.initialSol)n.cashSol=n.initialSol;paper.config=n;savePaper();res.json(paperPublicState());}catch(e){res.status(400).json({ok:false,error:safeError(e)});}});
app.post("/auto/start",requireAutoControl,(_req,res)=>{if(!JUPITER_API_KEY)return res.status(503).json({ok:false,error:"JUPITER_API_KEY absente : cotations indisponibles."});paper.config.enabled=true;paper.lastError=null;savePaper();res.json(paperPublicState());});
app.post("/auto/stop",requireAutoControl,(_req,res)=>{paper.config.enabled=false;savePaper();res.json(paperPublicState());});
app.post("/auto/reset",requireAutoControl,(req,res)=>{if(paper.positions.length)return res.status(409).json({ok:false,error:"Clôture les positions avant de réinitialiser le portefeuille."});const n=Math.max(.1,Math.min(100000,Number(req.body?.initialSol)||paper.config.initialSol));paper.config={...PAPER_DEFAULTS,initialSol:n,cashSol:n};paper.history=[];paper.lastError=null;savePaper();res.json(paperPublicState());});
setInterval(runPaperTrader,45000);setTimeout(runPaperTrader,12000);

const server = app.listen(PORT, () => {
  console.log("CART3R Helius scanner listening on", PORT, "mode=TEST realTrading=false");
  if (!HELIUS_RPC_URL) {
    state.scanner = "ERROR";
    state.lastError = "HELIUS_RPC_URL non configurée";
  } else {
    startHeliusSocket();
  }
  state.checkTimer = setInterval(runChecks, 15000);
});

process.on("SIGTERM", () => {
  state.running = false;
  clearTimeout(state.wsReconnectTimer);
  clearInterval(state.checkTimer);
  if (state.ws) { try { state.ws.close(); } catch (_) {} }
  server.close(() => process.exit(0));
});
