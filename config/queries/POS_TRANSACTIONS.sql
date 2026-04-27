
SELECT header.id, location.kepler_store_name, location.location_id, header.sale_datetime, header.new_customer, line_items.sku, line_items.is_aftercare, line_items.is_piercing, line_items.is_service, line_items.total_price_inc_tax, line_items.total_price
 FROM gold_analytics.f_lightspeed_sales AS header
INNER JOIN gold_common.d_location as location
    ON header.outlet_id = location.lightspeed_outlet_id
INNER JOIN gold_analytics.f_lightspeed_sale_line_items AS line_items
    ON header.id = line_items.sale_id 
WHERE header.sale_date >= add_months(current_date(), -4)
ORDER BY header.id, location.kepler_store_name, header.sale_datetime desc;
