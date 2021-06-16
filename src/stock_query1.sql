SELECT fin.sector, AVG(fin.trailingPE)
FROM stock_financials as fin
JOIN stock_tickers as tick
ON fin.symbol = tick.Symbol
WHERE fin.sector != "nan" AND tick.`Market Cap` > 0
GROUP BY fin.sector
ORDER BY AVG(fin.trailingPE) DESC;