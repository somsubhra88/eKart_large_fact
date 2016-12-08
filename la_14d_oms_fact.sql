INSERT overwrite TABLE la_14d_oms_fact
SELECT DISTINCT oiu.order_item_unit_quantity,
                oiu.order_item_quantity,
                oiu.order_item_selling_price,
                oiu.order_item_type,
                oiu.order_item_title,
                oiu.order_item_unit_status,
                oiu.order_item_unit_is_promised_date_updated,
                oiu.order_item_sub_type,
                oiu.order_id,
                oiu.order_item_id,
                oiu.order_item_unit_tracking_id,
                oiu.order_item_sku,
                oiu.order_item_category_id,
                oiu.order_item_sub_status,
                oiu.order_item_ship_group_id,
                oiu.order_item_status,
                oiu.order_item_unit_shipment_id,
                oiu.order_item_unit_id,
                oiu.order_item_unit_init_promised_datetime,
                oiu.order_item_product_id,
                oiu.order_item_unit_seller_id,
                oiu.order_item_unit_source_facility,
                oiu.order_item_listing_id,
                oiu.order_item_date,
                oiu.order_item_created_at,
                oiu.order_item_unit_final_promised_date_use,
                oiu.order_item_last_update,
                oiu.order_item_unit_new_promised_date,
                omso.`data`.order_shipping_address_id,
                omso.`data`.order_external_id AS order_external_id,
                omso.`data`.order_bill_to_party_id AS account_id,
                omsoi.order_item_max_approved_time,
                omsoi.order_item_max_on_hold_time,
                'forward' AS tracking_id_type,
                omsorder.order_original_billing_amount,
                omsdis.order_item_promotion_discount

FROM
  (SELECT is_large,
          product_categorization_hive_dim_key
   FROM bigfoot_external_neo.sp_product__product_categorization_hive_dim
   WHERE is_large=1) prod
LEFT OUTER JOIN
  (SELECT order_item_units.order_item_unit_quantity AS order_item_unit_quantity ,
          `data`.order_item_quantity AS order_item_quantity ,
          `data`.order_item_selling_price_in_paisa/100 AS order_item_selling_price ,
          `data`.order_item_type AS order_item_type,
          `data`.order_item_title AS order_item_title,
          order_item_units.order_item_unit_status AS order_item_unit_status ,
          IF (order_item_units.order_item_unit_new_promised_date IS NULL ,
                                                                    0,
                                                                    1) AS order_item_unit_is_promised_date_updated ,
             `data`.order_item_sub_type AS order_item_sub_type,
             `data`.order_id AS order_id,
             `data`.order_item_id AS order_item_id,
             order_item_units.order_item_unit_tracking_id AS order_item_unit_tracking_id,
             `data`.order_item_sku AS order_item_sku,
             `data`.order_item_category_id AS order_item_category_id,
             `data`.order_item_sub_status AS order_item_sub_status,
             `data`.order_item_ship_group_id AS order_item_ship_group_id,
             `data`.order_item_status AS order_item_status,
             order_item_units.order_item_unit_shipment_id AS order_item_unit_shipment_id,
             order_item_units.order_item_unit_id AS order_item_unit_id,
             from_unixtime(unix_timestamp(order_item_units.order_item_unit_promised_date)) AS order_item_unit_init_promised_datetime,
             `data`.order_item_fsn AS order_item_product_id,
             lookupkey('product_id',`data`.order_item_fsn) AS order_item_product_id_key,
             order_item_units.order_item_unit_seller_id,
             order_item_units.order_item_unit_fc AS order_item_unit_source_facility,
             `data`.order_item_listing_id,
             `data`.order_item_date,
             `data`.order_item_created_at,
             IF (order_item_units.order_item_unit_new_promised_date IS NULL,
                                                                       order_item_units.order_item_unit_promised_date ,
                                                                       order_item_units.order_item_unit_new_promised_date) AS order_item_unit_final_promised_date_use,
                updatedat AS order_item_last_update,
                order_item_units.order_item_unit_new_promised_date AS order_item_unit_new_promised_date
   FROM bigfoot_journal.dart_fkint_scp_oms_order_item_0_11_view_14d LATERAL VIEW explode(`data`.order_item_unit) exploded_table AS order_item_units
   WHERE `data`.order_item_status NOT IN ('created')) oiu ON prod.product_categorization_hive_dim_key=oiu.order_item_product_id_key
LEFT OUTER JOIN bigfoot_journal.dart_fkint_scp_oms_order_0_5_view_14d omso ON (omso.`data`.order_id = oiu.order_id)
LEFT OUTER JOIN
  (SELECT `data`.order_item_id AS omsoi_order_item_id,
          max(if(`data`.order_item_status = 'approved',from_utc_timestamp(updatedat,'GMT'),NULL)) AS order_item_max_approved_time,
          max(if(`data`.order_item_status = 'on_hold',from_utc_timestamp(updatedat,'GMT'),NULL)) AS order_item_max_on_hold_time
   FROM bigfoot_journal.dart_fkint_scp_oms_order_item_0_11_14d
   GROUP BY `data`.order_item_id) omsoi ON oiu.order_item_id = omsoi.omsoi_order_item_id

LEFT OUTER JOIN
(SELECT `data`.order_id AS order_id,
       `data`.order_original_billing_amount_in_paisa/100 AS order_original_billing_amount
FROM bigfoot_snapshot.dart_fkint_scp_oms_order_0_5_view_total) omsorder on omsorder.order_id = oiu.order_id

LEFT OUTER JOIN
(SELECT order_item_units.order_item_unit_id AS order_item_unit_id ,
       aggregate_filter('SUM', `data`.order_item_adjustment, 'order_item_adjustment_amount_in_paisa', 'order_item_adjustment_type', 'EQ', 'PROMOTION_DISCOUNT') / 100 AS order_item_promotion_discount
FROM bigfoot_snapshot.dart_fkint_scp_oms_order_item_0_11_view LATERAL VIEW explode(`data`.order_item_unit) exploded_table AS order_item_units) omsdis
ON omsdis.order_item_unit_id = oiu.order_item_unit_id;