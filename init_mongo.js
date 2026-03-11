db = db.getSiblingDB("financial_dre");

db.createCollection("instruments");
db.instruments.createIndex({ symbol: 1 }, { unique: true });
db.instruments.createIndex({ asset_class: 1 });
db.instruments.createIndex({ underlying: 1 });

db.instruments.insertMany([
  {
    symbol: "AAPL", instrument_id: "US0378331005", asset_class: "equity",
    currency: "USD", exchange: "NASDAQ", tick_size: 0.01, lot_size: 1,
    volatility_30d: 0.2234, volatility_252d: 0.2156, beta: 1.12,
    var_limit_1d: 500000, position_limit: 100000
  },
  {
    symbol: "MSFT", instrument_id: "US5949181045", asset_class: "equity",
    currency: "USD", exchange: "NASDAQ", tick_size: 0.01, lot_size: 1,
    volatility_30d: 0.1987, volatility_252d: 0.1923, beta: 0.95,
    var_limit_1d: 500000, position_limit: 100000
  },
  {
    symbol: "NVDA", instrument_id: "US67066G1040", asset_class: "equity",
    currency: "USD", exchange: "NASDAQ", tick_size: 0.01, lot_size: 1,
    volatility_30d: 0.4521, volatility_252d: 0.4389, beta: 1.72,
    var_limit_1d: 250000, position_limit: 50000
  },
  {
    symbol: "SPY", instrument_id: "US78462F1030", asset_class: "etf",
    currency: "USD", exchange: "NYSE", tick_size: 0.01, lot_size: 1,
    volatility_30d: 0.1423, volatility_252d: 0.1389, beta: 1.00,
    var_limit_1d: 1000000, position_limit: 500000
  },
  {
    symbol: "GLD", instrument_id: "US4642851001", asset_class: "etf",
    currency: "USD", exchange: "NYSE", tick_size: 0.01, lot_size: 1,
    volatility_30d: 0.1234, volatility_252d: 0.1201, beta: 0.05,
    var_limit_1d: 250000, position_limit: 100000
  }
]);

db.createCollection("correlation_matrices");
db.correlation_matrices.insertOne({
  name: "equity_desk_30d",
  as_of: new Date(),
  symbols: ["AAPL", "MSFT", "NVDA", "SPY", "GLD"],
  matrix: [
    [1.00, 0.78, 0.65, 0.82, 0.05],
    [0.78, 1.00, 0.71, 0.81, 0.03],
    [0.65, 0.71, 1.00, 0.74, 0.02],
    [0.82, 0.81, 0.74, 1.00, 0.08],
    [0.05, 0.03, 0.02, 0.08, 1.00]
  ]
});
