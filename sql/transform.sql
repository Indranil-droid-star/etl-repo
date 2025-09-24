CREATE OR REPLACE PROCEDURE transform_to_gold()
AS $$
BEGIN
    -- Truncate the Gold table
    TRUNCATE TABLE gold.transformed_data;

    -- Insert unique rows using ROW_NUMBER to remove duplicates based on order_id
    INSERT INTO gold.transformed_data (
        region, country, item_type, sales_channel, order_priority, order_date,
        order_id, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit, profit_margin
    )
    WITH deduped AS (
        SELECT
            region, country, item_type, sales_channel, order_priority, order_date,
            order_id, ship_date, units_sold, unit_price, unit_cost,
            total_revenue, total_cost, total_profit,
            CASE WHEN total_revenue > 0 THEN (total_profit / total_revenue) * 100 ELSE 0 END AS profit_margin,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
        FROM bronze.sales
    )
    SELECT
        region, country, item_type, sales_channel, order_priority, order_date,
        order_id, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit, profit_margin
    FROM deduped
    WHERE rn = 1;  -- Keep only the first row for each order_id
END;
$$ LANGUAGE plpgsql;
