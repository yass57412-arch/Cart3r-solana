
require("dotenv").config();

const express = require("express");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: "10kb" }));

let botStatus = {
  running: false,
  mode: "TEST",
  realTrading: false,
  scanner: "NOT_STARTED",
  positions: 0,
  lastUpdate: null
};

// Vérification du fonctionnement du serveur
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    name: "CART3R",
    mode: "TEST",
    time: new Date().toISOString()
  });
});

// État du moteur
app.get("/status", (req, res) => {
  res.json(botStatus);
});

// Démarrage du moteur de test uniquement
app.post("/test/start", (req, res) => {
  botStatus.running = true;
  botStatus.scanner = "WAITING";
  botStatus.lastUpdate = new Date().toISOString();

  res.json({
    ok: true,
    message: "CART3R test mode started",
    status: botStatus
  });
});

// Arrêt du moteur
app.post("/test/stop", (req, res) => {
  botStatus.running = false;
  botStatus.scanner = "STOPPED";
  botStatus.lastUpdate = new Date().toISOString();

  res.json({
    ok: true,
    message: "CART3R test mode stopped",
    status: botStatus
  });
});

// Aucun endpoint d'achat ou de vente réel à ce stade
app.listen(PORT, () => {
  console.log(`CART3R test server running on port ${PORT}`);
});
