SET SQL_SAFE_UPDATES = 0;

UPDATE  stock_tickers
SET `Last Sale` = REPLACE(REPLACE(`Last Sale`, '$', ''), ',','');

ALTER TABLE stock_tickers 
MODIFY COLUMN `Last Sale` DECIMAL(8,2);

with sector_pe_avg as (
SELECT fin.sector, AVG(fin.trailingPE) as sector_trailingPE
FROM stock_financials as fin
JOIN stock_tickers as tick
ON fin.symbol = tick.Symbol
WHERE fin.sector != "nan" AND tick.`Market Cap` > 0
GROUP BY fin.sector
ORDER BY AVG(fin.trailingPE) DESC
),
industry_pe_avg as (
SELECT fin.industry, AVG(fin.trailingPE) as industry_trailingPE
FROM stock_financials as fin
JOIN stock_tickers as tick
ON fin.symbol = tick.Symbol
WHERE fin.industry != "nan" AND tick.`Market Cap` > 0
GROUP BY fin.industry
ORDER BY AVG(fin.trailingPE) DESC
)

SELECT stock_financials.*, sector_pe_avg.*, industry_pe_avg.*, stock_tickers.`Market Cap`, stock_tickers.`Last Sale`
FROM stock_financials
JOIN sector_pe_avg 
ON sector_pe_avg.sector = stock_financials.sector
JOIN industry_pe_avg
ON industry_pe_avg.industry = stock_financials.industry
JOIN stock_tickers 
ON stock_tickers.Symbol = stock_financials.symbol
WHERE trailingPE IS NOT NULL
AND trailingPE < sector_trailingPE
AND trailingPE < industry_trailingPE
AND `Last Sale` < twoHundredDayAverage
ORDER BY `Market Cap` DESC;







