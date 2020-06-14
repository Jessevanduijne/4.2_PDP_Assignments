-- Get CSVExcelStorage
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- Load CSV-file
orders = LOAD '/user/maria_dev/Diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

-- Filter on target 'Holland'. Filtering is done before Group to make the query faster.
orders_filtered = FILTER orders BY target == 'Holland';

-- Group by “location” 
orders_grouped = GROUP orders_filtered BY (location, target);

-- Count how many times Holland was the target from that location
orders_unordered = FOREACH orders_grouped GENERATE group, COUNT(orders_filtered);

-- Make a alphabetic list from all locations from the orders.csv
orders_ordered = ORDER orders_unordered BY $0 ASC;

-- Print with DUMP-statement:
DUMP orders_ordered;
