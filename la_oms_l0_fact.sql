Insert overwrite table la_oms_l0_fact
Select 
order_item_replace_adjustment,	
order_item_unit_discount,	
order_item_unit_approve_verification_cdays,	
order_item_unit_quantity,	
order_item_unit_approve_verification_time,	
order_item_promotion_credit,	
order_item_unit_approve_delivery_bdays,	
order_item_shipping_discount,	
order_item_unit_final_sales_amount,	
order_item_dummy_value,	
order_item_unit_approve_confirm_time,	
order_item_unit_approve_verification_bdays,	
order_item_unit_approve_delivery_time,	
order_item_octroi_fee,	
order_original_billing_amount,	
order_item_binding_fee,	
order_item_cashback_amount,	
order_item_list_price,	
order_item_quantity,	
order_item_unit_approve_dispatch_cdays,	
order_item_shipping_charges,	
order_item_unit_approve_rts_bdays,	
order_item_unit_approve_cancel_bdays,	
order_item_selling_price,	
order_item_promotion_discount,	
order_item_bundle_discount,	
order_item_unit_approve_rts_cdays,	
order_item_unit_breach_delivery_bdays,	
order_item_sourcing_fee,	
order_item_pickup_charge,	
order_item_unit_approve_ship_time,	
order_item_unit_approve_cancel_cdays,	
order_item_unit_approve_ship_bdays,	
order_item_unit_approve_delivery_cdays,	
order_item_bank_offer_amount,	
order_item_exchange_discount,	
order_item_cod_charges,	
order_item_unit_approve_ship_cdays,	
order_item_offers_snapshot_effective_price_change,	
order_adjustment_amount,	
order_item_freebie_discount,	
order_item_unit_approve_dispatch_bdays,	
order_item_unit_approve_confirm_bdays,	
order_item_unit_breach_delivery_cdays,	
order_item_adjustment_amount,	
order_item_price_change,	
order_item_unit_breach_delivery_time,	
order_item_fee,	
order_item_unit_approve_rts_time,	
order_item_unit_approve_confirm_cdays,	
call_verification_item_quantity,	
order_item_unit_approve_dispatch_time,	
order_item_unit_approve_cancel_time,	
order_item_is_gift,	
order_item_is_exchange as is_exchange,
order_item_id,	
order_external_id,	
order_item_is_replacement as is_replacement,
order_tenant_id,	
order_sub_channel,	
order_item_is_call_verified,	
order_item_is_inventory,	
order_token_id,	
call_verification_comments,	
order_item_unit_promise_breach,	
order_item_sales_reporting_date_time,	
order_item_ship_group_id,	
order_sales_channel,	
order_item_sub_type,	
order_item_unit_status,	
order_item_unit_shipment_id,	
call_verification_type,	
order_item_category_id,	
order_terminal_id,	
order_item_is_pre_order,	
order_user_ip,	
order_item_is_freebie,	
order_item_is_back_order,	
order_item_unit_tracking_id,	
order_item_status,	
order_item_approve_date_time,	
order_id,	
order_created_by,	
order_item_type,	
order_item_po_item_id,	
order_user_agent,	
order_item_unit_is_promised_date_updated,	
order_item_offers_snapshot_offer_type,	
order_item_title,	
order_item_sub_status,	
order_item_unit_dispatch_date_time,	
order_item_unit_is_on_hold,	
order_item_unit_id,	
order_item_offers_snapshot_external_id,	
order_status,	
order_item_unit_sla,	
call_verification_reason,	
order_item_unit_is_rto,	
call_verification_status,	
order_item_service_profile,	
order_item_is_duplicate as is_duplicate,
order_item_internal_sla,	
order_item_sku,	
order_item_verification_un_hold_max_date_key,	
order_item_un_hold_date_key,	
order_item_unit_ship_date_key,	
order_item_approve_time_key,	
order_item_sales_reporting_date_key,	
order_item_unit_request_courier_return_date_key,	
order_item_non_verification_on_hold_time_key,	
order_item_un_hold_max_date_key,	
order_item_unit_confirm_time_key,	
order_item_non_verification_on_hold_max_time_key,	
order_item_unit_cancel_date_key,	
order_item_unit_cancel_time_key,	
order_item_un_hold_time_key,	
order_item_non_verification_un_hold_max_date_key,	
order_item_verification_on_hold_max_date_key,	
order_item_unit_dispatch_date_key,	
order_item_unit_dispatch_time_key,	
order_billing_address_id_key,	
order_item_unit_cancel_customer_return_time_key,	
order_item_verification_un_hold_time_key,	
order_item_verification_un_hold_date_key,	
order_item_listing_id_key,	
order_item_listing_id,	
order_item_unit_init_promised_date_key,	
order_item_seller_id_key,	
order_item_unit_deliver_time_key,	
order_item_verification_on_hold_time_key,	
order_item_sales_reporting_time_key,	
order_item_unit_source_facility_id_key as order_item_unit_source_facilty_id_key,
order_item_non_verification_un_hold_time_key,	
order_item_unit_return_time_key,	
order_item_non_verification_on_hold_max_date_key,	
order_item_unit_confirm_date_key,	
order_item_un_hold_max_time_key,	
order_item_verification_un_hold_max_time_key,	
order_item_unit_logistics_vendor_key,	
order_shipping_address_id_key as order_pincode_key,
order_item_unit_final_promised_date_key,	
order_item_non_verification_on_hold_date_key,	
order_item_verification_on_hold_date_key,	
order_item_unit_cancel_courier_return_time_key,	
order_item_unit_cancel_customer_return_date_key,	
order_item_unit_cancel_courier_return_date_key,	
order_item_unit_ship_time_key,	
account_id_key,	
order_item_unit_return_date_key,	
order_item_unit_ready_to_ship_date_key,	
order_item_non_verification_un_hold_max_time_key,	
order_item_approve_date_key,	
order_item_product_id_key,	
order_item_verification_on_hold_max_time_key,	
order_item_unit_request_courier_return_time_key,	
order_item_unit_request_customer_return_date_key,	
order_item_unit_request_customer_return_time_key,	
order_item_unit_deliver_date_key,	
order_item_unit_ready_to_ship_time_key,	
order_item_non_verification_un_hold_date_key,	
order_item_approve_max_date_time as order_item_max_approved_time,
order_item_approve_max_date_key,	
order_item_approve_max_time_key,	
order_item_date,	
order_item_created_at,	
order_shipping_address_pincode_key,	
order_item_unit_approve_promise_bdays,	
order_item_unit_approve_promise_cdays,	
order_item_rts_due_date_key,	
order_item_rts_due_time_key,	
order_item_rts_due_date_time,	
order_item_unit_rts_breach,	
oms.account_id,
order_item_product_id,	
order_item_seller_id,	
order_date_time,	
order_sales_channel_generic,	
order_sales_channel_windows_split,	
order_item_promise_tier,	
order_last_payment_gateway,	
order_last_payment_status,	
order_last_payment_method,	
order_all_payment_methods,	
order_payment_updated_date_time,	
order_payment_type,	
flipkart_first_flag,	
net_filter_flag,	
seller_oms_payment_approved_date_key,	
seller_oms_packed_date_key,	
seller_oms_packed_in_progress_date_key,	
seller_oms_rts_oms_in_progress_date_key,	
seller_oms_rts_oms_done_date_key,	
seller_oms_to_dispatch_date_key,	
seller_oms_pickup_complete_date_key,	
seller_oms_done_date_key,	
seller_oms_label_created_date_key,	
seller_oms_cancelled_date_key,	
seller_oms_delivered_date_key,	
seller_oms_completed_date_key,	
seller_oms_shipped_date_key,	
seller_oms_cancelled_in_progress_date_key,	
fulfill_item_unit_dispatch_service_tier,	
order_billing_amount,	
importance_type,	
is_core,	
cluster,	
newcustomer_flag,	
listing_quality_score,	
order_item_unit_init_promised_date_time as order_item_unit_init_promised_datetime,
order_item_unit_final_promised_date_time as order_item_unit_final_promised_date_use,
fa_assured_flag,	
order_item_unit_approve_date_time,	
order_item_shipment_type,	
lookup_date(to_date(order_item_Date)) as order_item_date_key,
lookup_time(to_date(order_item_Date)) as order_item_time_key,
cp_user.address_pincode as address_pincode
from bigfoot_external_neo.scp_oms__order_item_unit_s1_fact oms
JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim product
ON product.product_categorization_hive_dim_key=oms.order_item_product_id_key
LEFT JOIN bigfoot_external_neo.cp_user__address_hive_dim as cp_user
on cp_user.address_hive_dim_key = oms.order_shipping_address_id_key
WHERE product.is_large=1;