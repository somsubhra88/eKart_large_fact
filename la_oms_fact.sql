INSERT overwrite TABLE la_oms_fact
SELECT DISTINCT order_item_unit_id,
                order_item_id,
                order_item_unit_tracking_id,
                order_item_unit_shipment_id,
                order_item_unit_quantity,
                order_item_quantity,
                order_item_selling_price,
                order_item_type,
                order_item_title,
                order_item_unit_status,
                order_item_unit_is_promised_date_updated,
                order_item_unit_new_promised_date,
                order_item_sub_type,
                order_id,
                order_item_sku,
                order_item_category_id,
                order_item_sub_status,
                order_item_ship_group_id,
                order_item_status,
                order_item_product_id_key,
                order_item_unit_init_promised_date_key,
                order_item_date_key,
                order_item_unit_final_promised_date_key,
                order_item_seller_id_key,
                order_item_time_key,
                order_item_unit_source_faciltiy,
                order_item_listing_id_key,
                order_item_unit_final_promised_date_use,
                order_item_unit_init_promised_datetime,
                order_item_last_update,
                order_item_date,
                order_item_created_at,
                order_shipping_address_id_key,
                order_external_id,
                call_verification_reason,
                order_item_max_approved_time,
                order_item_max_on_hold_time,
                order_last_payment_method,
                call_verification_type,
                is_replacement,
                is_exchange,
                is_duplicate,
                account_id,
                account_id_key,
                CASE
                    WHEN contact_date_key IS NOT NULL THEN CASE
                                                               WHEN order_item_unit_delivered_date_key IS NULL THEN 1
                                                               ELSE CASE
                                                                        WHEN (contact_date_key < order_item_unit_delivered_date_key) THEN 1
                                                                        WHEN (contact_date_key = order_item_unit_delivered_date_key
                                                                              AND contact_time_key < order_item_unit_delivered_time_key) THEN 1
                                                                        ELSE 0
                                                                    END
                                                           END
                    ELSE 0
                END AS customer_contact_flag,
                cu_issue_type AS issue_type,
                order_shipping_address_id,
                calculated_delivery_hub,
                order_pincode_key,
                cu_sub_issue_type,
                incident_creation_time,
                cu_sub_sub_issue_type AS dummy
FROM
  (SELECT oiu.order_item_unit_quantity,
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
          oiu.order_item_product_id_key,
          oiu.order_item_unit_init_promised_date_key,
          oiu.order_item_date_key,
          oiu.order_item_unit_final_promised_date_key,
          oiu.order_item_seller_id_key,
          oiu.order_item_time_key,
          oiu.order_item_unit_source_faciltiy,
          oiu.order_item_listing_id_key,
          oiu.order_item_date,
          oiu.order_item_created_at,
          oiu.order_item_unit_final_promised_date_use,
          oiu.order_item_last_update,
          oiu.order_item_unit_new_promised_date,
          lookupkey('address_id',omso.order_shipping_address_id) AS order_shipping_address_id_key,
          omso.order_external_id AS order_external_id,
          lookupkey('account_id',omso.order_bill_to_party_id) AS account_id_key,
          omso.order_bill_to_party_id AS account_id,
          cv.call_verification_type AS call_verification_type,
          cv.call_verification_reason AS call_verification_reason,
          omsoi.order_item_max_approved_time,
          omsoi.order_item_max_on_hold_time,
          (CASE WHEN oms_pay.order_last_payment_method = 'COD' THEN 'COD' ELSE 'Prepaid' END) AS order_last_payment_method,
          if(assoc.order_item_is_replacement>0,1,0) AS is_replacement,
          if(assoc.order_item_is_duplicate>0,1,0) AS is_duplicate,
          if(assoc.order_item_is_exchange>0,1,0) AS is_exchange,
          omso.order_shipping_address_id AS order_shipping_address_id,
          cust_hub.calculated_delivery_hub,
          cust_add.order_pincode_key,
          lookup_date(omsoi.order_item_unit_delivered_datetime) AS order_item_unit_delivered_date_key,
          lookup_time(omsoi.order_item_unit_delivered_datetime) AS order_item_unit_delivered_time_key,
          cu.contact_date_key,
          cu.contact_time_key,
          cu.cu_issue_type,
          cu.cu_sub_issue_type,
          cu.cu_sub_sub_issue_type,
          cu.incident_creation_time
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
                lookupkey('product_id', `data`.order_item_fsn) AS order_item_product_id_key,
                lookup_date(order_item_units.order_item_unit_promised_date) AS order_item_unit_init_promised_date_key,
                lookup_date(`data`.order_item_date) AS order_item_date_key,
                lookup_date(IF (order_item_units.order_item_unit_new_promised_date IS NULL ,order_item_units.order_item_unit_promised_date ,order_item_units.order_item_unit_new_promised_date)) AS order_item_unit_final_promised_date_key,
                lookupkey('seller_id', order_item_units.order_item_unit_seller_id) AS order_item_seller_id_key,
                lookup_time(`data`.order_item_date) AS order_item_time_key,
                order_item_units.order_item_unit_fc AS order_item_unit_source_faciltiy,
                lookupkey('listing_id', `data`.order_item_listing_id) AS order_item_listing_id_key,
                `data`.order_item_date,
                `data`.order_item_created_at,
                IF (order_item_units.order_item_unit_new_promised_date IS NULL,
                                                                          order_item_units.order_item_unit_promised_date ,
                                                                          order_item_units.order_item_unit_new_promised_date) AS order_item_unit_final_promised_date_use,
                   cast(cast(updatedat/1000 AS TIMESTAMP) AS STRING) AS order_item_last_update,
                   order_item_units.order_item_unit_new_promised_date AS order_item_unit_new_promised_date
      FROM bigfoot_snapshot.dart_fkint_scp_oms_order_item_0_11_view_total LATERAL VIEW explode(`data`.order_item_unit) exploded_table AS order_item_units
      WHERE `data`.order_item_status NOT IN ('created')
        AND updatedat > ((UNIX_TIMESTAMP()-(86400*180))*1000)) oiu ON prod.product_categorization_hive_dim_key=oiu.order_item_product_id_key
   LEFT OUTER JOIN
     (SELECT `data`.order_shipping_address_id AS order_shipping_address_id,
             `data`.order_external_id AS order_external_id,
             `data`.order_bill_to_party_id AS order_bill_to_party_id,
             `data`.order_id AS order_id
      FROM bigfoot_snapshot.dart_fkint_scp_oms_order_0_5_view_total
      WHERE updatedat > ((UNIX_TIMESTAMP()-(86400*180))*1000)) omso ON (omso.order_id = oiu.order_id)
   LEFT OUTER JOIN
     (SELECT order_item_id,
             updated_at,
             call_verification_type,
             call_verification_reason,
             row_desc
      FROM
        (SELECT `data`.call_verification_items[0].call_verification_item_order_item_id AS order_item_id,
                `data`.call_verification_updated_at AS updated_at,
                LAST_VALUE(`data`.call_verification_type) OVER (PARTITION BY `data`.call_verification_items[0].call_verification_item_order_item_id
                                                                ORDER BY `data`.call_verification_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS call_verification_type,
                LAST_VALUE(`data`.call_verification_reason) OVER (PARTITION BY `data`.call_verification_items[0].call_verification_item_order_item_id
                                                                  ORDER BY `data`.call_verification_updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS call_verification_reason,
                row_number() OVER (PARTITION BY `data`.call_verification_items[0].call_verification_item_order_item_id
                                   ORDER BY `data`.call_verification_updated_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS row_desc
         FROM bigfoot_snapshot.dart_fkint_scp_oms_call_verification_0_view_total
         WHERE updatedat > ((UNIX_TIMESTAMP()-(86400*180))*1000))call_verif
      WHERE call_verif.row_desc = '1') cv ON (cv.order_item_id = oiu.order_item_id)
   LEFT OUTER JOIN
     (SELECT oi_units.order_item_unit_id AS omsoi_order_item_unit_id,
             max(if(`data`.order_item_status = 'approved',from_utc_timestamp(updatedat,'GMT'),NULL)) AS order_item_max_approved_time,
             max(if(`data`.order_item_status = 'on_hold',from_utc_timestamp(updatedat,'GMT'),NULL)) AS order_item_max_on_hold_time,
             min(if(oi_units.order_item_unit_status = 'delivered',oi_units.order_item_unit_updated_at,NULL)) AS order_item_unit_delivered_datetime
      FROM bigfoot_journal.dart_fkint_scp_oms_order_item_0_11 LATERAL VIEW explode(`data`.order_item_unit) exploded_table AS oi_units
      WHERE DAY > #200#DAY#
      GROUP BY oi_units.order_item_unit_id) omsoi ON (oiu.order_item_unit_id = omsoi.omsoi_order_item_unit_id)
   LEFT OUTER JOIN
     (SELECT `data`.order_item_assoc_to_order_item AS order_item_assoc_to_order_item,
             sum(if(`data`.Order_Item_Assoc_Type = 'replacement',1,0)) AS order_item_is_replacement,
             sum(if(`data`.Order_Item_Assoc_Type = 'duplicate',1,0)) AS order_item_is_duplicate,
             sum(if(`data`.Order_Item_Assoc_Type = 'exchange',1,0)) AS order_item_is_exchange
      FROM bigfoot_snapshot.dart_fkint_scp_oms_order_item_assoc_2_view_total
      WHERE updatedat > ((UNIX_TIMESTAMP()-(86400*180))*1000)
      GROUP BY `data`.order_item_assoc_to_order_item) assoc ON (assoc.order_item_assoc_to_order_item = oiu.order_item_id)
   LEFT JOIN
     (SELECT tmp.order_external_id AS cu_order_external_id,
             COALESCE(lookupkey('product_id', tmp.order_item_fsn), 'noData') AS issue_fsn_key,
             tmp.issue_creation_date_key AS contact_date_key,
             tmp.issue_creation_time_key AS contact_time_key,
             tmp.issue_type AS cu_issue_type,
             tmp.sub_issue_type AS cu_sub_issue_type,
             tmp.sub_sub_issue_type AS cu_sub_sub_issue_type,
             tmp.incident_creation_time AS incident_creation_time
      FROM
        (SELECT first_value(a.incident_id) OVER (PARTITION BY a.order_external_id,a.order_item_fsn
                                                 ORDER BY a.incident_creation_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS incident_id,
                a.order_external_id,
                a.issue_type,
                a.sub_issue_type,
                a.sub_sub_issue_type,
                a.order_item_fsn,
                a.incident_creation_time,
                a.issue_creation_date_key,
                a.issue_creation_time_key,
                row_number() Over (PARTITION BY a.order_external_id,a.order_item_fsn
                                   ORDER BY a.incident_creation_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS row_n
         FROM
           (SELECT ims_fact.incident_id,
                   ims_fact.order_external_id,
                   ims_fact.issue_type,
                   ims_fact.sub_issue_type,
                   ims_fact.sub_sub_issue_type,
                   ims_fact.order_item_fsn,
                   ims_fact.incident_creation_time,
                   lookup_date(ims_fact.incident_creation_time) AS issue_creation_date_key,
                   lookup_time(ims_fact.incident_creation_time) AS issue_creation_time_key
            FROM bigfoot_external_neo.mp_cs__ims_incident_hive_fact ims_fact
            WHERE ims_fact.sub_issue_type IN ('MP Cancellation-ESC',
                                              'Customer never rejected',
                                              'No attempt Done',
                                              'Call not received before delivery attempt',
                                              'Wrong Update by FE',
                                              'Wrong Commitment by FE',
                                              'FE misbehavior against women',
                                              'FE contacted customer for non-officially',
                                              'FE in possession of liquor/drugs',
                                              'FE misbehaviour women- Sexual harassment',
                                              'FE misbehaviour against children',
                                              'FE Misbehaviour against women',
                                              'FE contacted customer for non-officially',
                                              'FE in possession of liquor/drugs',
                                              'FE misbehaviour against children',
                                              'FE misbehaviour women- Sexual harassment',
                                              'FE refused doorstep delivery',
                                              'FE grooming issues',
                                              'FE in a hurry',
                                              'FE asked for tip',
                                              'FE did not have/give change',
                                              'FE refused doorstep delivery',
                                              'FE in a hurry',
                                              'FE did not have/give change',
                                              'FE asked for tip',
                                              'OOS_Cancellation',
                                              'Revised SLA Notification',
                                              'OOS_Alternate item',
                                              'Approved but not dispatched',
                                              'Packed but not shipped',
                                              'Dispatched but not shipped',
                                              'My Order is delayed',
                                              'Other( order delivery)',
                                              'Delivery Delay EKL-ESC',
                                              'Complaints on Delivery Executive-ESC',
                                              'Out for Delivery EKL-ESC',
                                              'Ontime Shipments EKL-ESC',
                                              'FE Helpline not answering',
                                              'Delivery Status - Pre SLA',
                                              'Delivery Status - Out for Delivery',
                                              'Reattempt Delivery',
                                              'Delivery Delay - Post SLA',
                                              'Others',
                                              'Wrong Delivery',
                                              'Unable to track',
                                              'Delivery Status - Post SLA',
                                              'Delivery Status - Pre SLA',
                                              'Delivery Status - Out for delivery',
                                              'Reattempt delivery',
                                              'Delivery Delay - Post SLA',
                                              'Wrong Delivery',
                                              'Next Day Delivery - Post SLA',
                                              'Call before delivery',
                                              'Unable to track',
                                              'FE Misbehaviour',
                                              'POS related',
                                              'Deliver After Date',
                                              'SDD - Post SLA',
                                              'Next Day Delivery - Pre SLA',
                                              'Delivery Status - Post SLA',
                                              'Next Day Delivery - Out for Delivery',
                                              'SDD - Pre SLA',
                                              'SDD - Out for Delivery',
                                              'Delay in Delivery',
                                              'Cancelled - still delivered',
                                              'Order status related',
                                              'Delay',
                                              'Customer Requests for changes',
                                              'FK Cancelled Order',
                                              'Customer-initiated Cancellation',
                                              'Details required',
                                              'Delivery/FE related Feedback',
                                              'Product Specific Information',
                                              'Damaged in transit')
              AND ims_fact.issue_type IN ('FE - Misbehaviour FSD',
                                          'Report Abuse, Misconduct',
                                          'Order Delivery',
                                          'Order Modification',
                                          'Feedback',
                                          'Post Shipment-ESC',
                                          'Product Purchase Queries',
                                          'Shipped to Delivery-FSD',
                                          'Customer not aware of cancellation',
                                          'Dispute Resolution',
                                          'FE - Rude behaviour-FSD')
              AND ims_fact.sub_sub_issue_type IN ('Status check',
                                                  'Delay in Delivery',
                                                  'Customer requested Address Change',
                                                  'Customer not aware of cancellation',
                                                  'Address change denied',
                                                  'FE/Delivery Boy/Person details required',
                                                  'Procurement Delay',
                                                  'Customer requested to add alternate phone number',
                                                  'Incorrect address',
                                                  'Pick up center details required',
                                                  'Still in transit customer wants Delivery',
                                                  'Gift Wrap related Enquries',
                                                  'Customer wants delivery to some other person',
                                                  'Customer requested to gift wrap the item',
                                                  'Broken Product Received',
                                                  'Out of stock',
                                                  'Out of delivery area',
                                                  'Not serviceable pincode',
                                                  'Damaged shipment',
                                                  'Lost shipment',
                                                  'Shipment mis-route',
                                                  'Mis-shipped item',
                                                  'Logistics vendor not found',
                                                  'Cancelling due to Delay in Order Delivery',
                                                  'Change in different delivery date/time denied',
                                                  'Packed but not shipped',
                                                  'Ready to Shipped',
                                                  'Dispatched but Not shipped',
                                                  'Shipped but Misrouted',
                                                  'Wrong Delivery',
                                                  'Unable to track',
                                                  'Wrong update provided',
                                                  'wants to check why there is no progress on Order status',
                                                  'Wants to know the reason of re-promise',
                                                  'POS related issues',
                                                  'Complaints',
                                                  'Courier Charges Related Issues',
                                                  'FE misbehavior women- Sexual harassment',
                                                  'FE misbehavior against children',
                                                  'FE contacted customer for non-officially',
                                                  'FE in possession/influence of liquor/drugs',
                                                  'FE involved in physical abuse',
                                                  'Too many issues from specific FE')) a)tmp
      WHERE tmp.row_n=1) cu ON cu.cu_order_external_id = omso.order_external_id
   AND cu.issue_fsn_key = oiu.order_item_product_id_key
   LEFT OUTER JOIN
     (SELECT `data`.payment_ref_num_2 AS order_external_id,
             first_value(`data`.payment_method) over (partition BY `data`.payment_ref_num_2
                                                      ORDER BY `data`.updated_at DESC) AS order_last_payment_method
      FROM bigfoot_snapshot.dart_fkint_apl_finance_reporting_payment_1_2_view
      WHERE `data`.bu_id='FKMP_CUSTOMER'
        AND `data`.type IN ('CustomerPayment',
                            'CustomerCredit')
        AND ((`data`.payment_ref_num_3 IS NOT NULL
              AND `data`.payment_method="COD")
             OR (`data`.status = "received"
                 AND `data`.payment_method<>"COD"
                 AND `data`.payment_method<>"POS"))) oms_pay ON oms_pay.order_external_id=omso.order_external_id
   LEFT OUTER JOIN
     (SELECT entityid AS cust_add_entity_id,
             `data`.useraddress.pincode AS cust_add_pincode,
             lookupkey('pincode',`data`.useraddress.pincode) AS order_pincode_key,
             `data`.useraddress.addressid AS cust_add_id
      FROM bigfoot_snapshot.dart_fkint_cp_user_contact_2_view_total
      WHERE updatedat > ((UNIX_TIMESTAMP()-(86400*180))*1000)) cust_add ON omso.order_shipping_address_id=cust_add.cust_add_id
   LEFT OUTER JOIN
     (SELECT DISTINCT pincode AS customer_address_pincode,
                      route AS customer_pincode_route,
                      hub_name AS calculated_delivery_hub,
                      hub_zone AS customer_pincode_hub_zone
      FROM bigfoot_common.large_pincode_hub_mapping) cust_hub ON cust_hub.customer_address_pincode=cust_add.cust_add_pincode) FINAL
WHERE order_item_unit_source_faciltiy IS NULL
  OR order_item_unit_source_faciltiy LIKE '%0_L'
  OR order_item_unit_source_faciltiy='c82e1fb314f34969';