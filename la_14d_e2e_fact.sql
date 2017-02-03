insert overwrite table la_14d_e2e_fact
select
order_item_unit_quantity, 
order_item_quantity, 
order_item_selling_price, 
order_item_type, 
order_item_title, 
order_item_unit_status, 
order_item_unit_is_promised_date_updated, 
order_item_sub_type, 
order_id, 
order_item_id, 
order_item_unit_tracking_id, 
order_item_sku, 
order_item_category_id, 
order_item_sub_status, 
order_item_ship_group_id, 
order_item_status, 
order_item_unit_shipment_id, 
order_item_unit_id, 
order_item_unit_init_promised_datetime, 
order_item_product_id, 
order_item_product_id_key, 
order_item_unit_seller_id, 
order_item_unit_source_facility, 
order_item_listing_id, 
order_item_date, 
order_item_date_key, 
order_item_created_at, 
order_item_unit_final_promised_date_use, 
order_item_unit_final_promised_date_key, 
order_item_last_update, 
order_item_unit_new_promised_date, 
order_shipping_address_id, 
order_shipping_address_id_key, 
order_external_id, 
account_id, 
order_item_max_approved_time, 
order_item_max_on_hold_time, 
tracking_id_type, 
customer_promise_proactive_age, 
fulfill_item_unit_dispatch_actual_datetime, 
fulfill_item_unit_dispatch_actual_date_key, 
fulfill_item_unit_ship_actual_time, 
fulfill_item_unit_dispatch_expected_datetime, 
fulfill_item_unit_dispatch_expected_date_key, 
fulfill_item_unit_ship_expected_time, 
fulfillment_order_item_id, 
fulfill_item_unit_id, 
fulfill_item_unit_status, 
shipment_merchant_reference_id, 
fulfill_item_unit_updated_at, 
fulfill_item_unit_region, 
fulfill_item_unit_region_type, 
fulfill_item_id, 
fulfill_item_type, 
fulfill_item_unit_deliver_after_time, 
fulfill_item_unit_dispatch_after_time, 
fulfill_item_unit_is_for_slotted_delivery, 
fulfill_item_unit_reserved_status_b2c_actual_time, 
fulfill_item_unit_reserved_status_expected_time, 
fulfill_item_unit_delivered_status_expected_time, 
fulfill_item_unit_delivered_status_id, 
fulfill_item_unit_size, 
fulfill_item_unit_reserved_status_b2b_actual_time, 
slot_changed_created_at, 
slot_changed_by, 
slot_booking_ref_id, 
customer_destination_pincode, 
fc_breach_flag, 
fc_pending_flag, 
fc_pending_age, 
shipment_id, 
vendor_tracking_id, 
shipment_current_status, 
shipment_current_status_datetime, 
payment_type, 
destination_pincode, 
ekl_shipment_type, 
customer_promise_datetime, 
logistics_promise_datetime, 
shipment_created_at_datetime, 
shipment_created_at_date_key, 
vendor_id, 
vendor_id_key, 
shipment_carrier, 
fsd_assigned_hub_id,
fsd_assigned_hub_id_key, 
fsd_last_current_hub_id, 
fsd_last_current_hub_id_key, 
fsd_last_current_hub_type, 
ekl_billable_weight, 
merchant_reference_id, 
shipment_first_consignment_id, 
shipment_last_consignment_id, 
shipment_inscan_time, 
fsd_last_received_time, 
fsd_number_of_ofd_attempts, 
fsd_assignedhub_expected_time, 
fsd_assignedhub_received_time, 
ekl_delivery_datetime, 
ekl_delivery_date_key, 
shipment_first_ofd_datetime, 
shipment_last_ofd_datetime, 
vendor_dispatch_datetime, 
tpl_first_ofd_time, 
tpl_last_ofd_time, 
fsd_assigned_hub_sent_datetime, 
openbox_reject_datetime, 
merchant_id, 
fsd_first_dh_received_datetime, 
shipment_value, 
runsheet_close_datetime, 
COD_amount_to_collect, 
shipment_first_consignment_create_datetime, 
shipment_last_consignment_create_datetime, 
shipment_origin_facility_id, 
end_state_datetime, 
shipment_first_consignment_conn_id, 
shipment_last_consignment_conn_id, 
shipment_first_consignment_eta_in_sec, 
shipment_last_consignment_eta_in_sec, 
fsd_current_customer_dependency_flag, 
shipment_dh_pending_flag, 
ekl_pending_flag, 
openbox_reject_flag, 
shipment_dh_days_for_delivery, 
first_attempt_delivery_flag, 
slot_id, 
slot_code, 
slot_facility_id, 
slot_facility_id_key, 
slot_start_date, 
slot_start_date_key, 
slot_end_date, 
slot_actual_capacity, 
slot_shipment_size, 
slot_duration, 
slot_booking_id, 
slot_reservation_status, 
product_categorization_hive_dim_key, 
order_item_product_title, 
analytic_super_category, 
order_item_brand, 
analytic_vertical, 
analytic_category, 
slot_facility_name, 
slot_facility_zone, 
slot_facility_type, 
fsd_last_current_hub_name, 
fsd_assigned_hub_name, 
fsd_assigned_hub_zone, 
fsd_assigned_hub_type, 
shipment_vendor_name, 
present_day, 
present_time, 
fc_breach_bucket, 
shipment_dh_fwd_promised_date, 
shipment_pending_location, 
shipment_breach_flag, 
pending_beyond_customer_promise_age, 
asset,
(CASE
WHEN asset in ('12_Returned','11_Delivered','13_Cancelled')
THEN 0
WHEN asset in ('01_OMS') and unix_timestamp() > unix_timestamp(order_item_date) + 86400
THEN 1
WHEN asset in ('02_Fulfillment') and unix_timestamp() > unix_timestamp(order_item_max_approved_time) + 14400
THEN 1
WHEN asset in ('03_Warehouse') and unix_timestamp() > unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)
THEN 1
WHEN asset in ('10_3PL') and unix_timestamp() > unix_timestamp(order_item_unit_final_promised_date_use)
THEN 1
WHEN asset in ('04_MotherHub') and unix_timestamp() > unix_timestamp(mh_cutoff_datetime)
THEN 1
WHEN asset in ('05_Transport') and unix_timestamp() > unix_timestamp(shipment_last_consignment_create_datetime) + tpt_time_diff_in_sec + shipment_last_consignment_eta_in_sec
THEN 1
WHEN asset in ('06_DeliveryHub') and unix_timestamp() > unix_timestamp(shipment_dh_fwd_promised_date)
THEN 1
ELSE 0
END) as current_asset_breach_flag,
mh_cutoff_datetime,
customer_destination_pincode_key,
calculated_delivery_hub,
shipment_origin_facility_id_key,
fulfill_item_unit_final_reserved_datetime,
fulfill_item_unit_final_reserved_date_key,
fsd_assigned_hub_expected_receive_datetime,
calculated_delivery_hub_zone,
first_conn_cutoff_in_sec,
last_conn_cutoff_in_sec,
tpt_time_diff_in_sec,
customer_address_pincode,
customer_pincode_hub,
customer_pincode_hub_zone,
customer_pincode_route,
is_replacement,
is_duplicate,
is_exchange,
order_payment_type,
onhold_reason,
onhold_sub_reason,
if(fulfill_item_unit_region like '%0_L','1','0') as dummy1,
NULL as dummy2,
customer_dependency_flag,
IF(
	from_unixtime(unix_timestamp(),'yyyy-MM-dd') <= to_date(order_item_unit_final_promised_date_use),'10_Future Shipments',
		IF(
			NOT(isnull(shipment_rto_create_time)) and to_date(shipment_rto_create_time) <= to_date(order_item_unit_final_promised_date_use), '09_RTO_Before_CPD',
				IF(
					NOT(isnull(ekl_delivery_datetime)) and to_date(ekl_delivery_datetime) <= to_date(order_item_unit_final_promised_date_use),'09_Delivered_by_Promise',
						IF(
							NOT(isnull(shipment_first_ofd_datetime)) and to_date(shipment_first_ofd_datetime) <= to_date(order_item_unit_final_promised_date_use) and customer_dependency_flag=1,'08_Customer_Dependency_OFD_On_Promise',
								IF(
									to_date(logistics_promise_datetime)>to_date(order_item_unit_final_promised_date_use),'07_LPD>CPD - Tech_Issue',
										IF(
											from_unixtime(unix_timestamp(fulfill_item_unit_reserved_status_b2c_actual_time)+(150 * 60))> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)),'01_Fulfilment_Breach',
												IF(
													from_unixtime(unix_timestamp(order_item_max_on_hold_time)+(150 * 60))> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)),'02_CS_Breach',
														IF(
															isnull(fulfill_item_unit_dispatch_actual_datetime) AND from_unixtime(unix_timestamp())> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)),'03_DC_Breach',
																IF(
																	NOT(isnull(fulfill_item_unit_dispatch_actual_datetime))	AND from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_actual_datetime)) > from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)),'03_DC_Breach',
																		IF(
																			isnull(shipment_first_consignment_create_datetime) AND from_unixtime(unix_timestamp())> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)+(120 * 60)),'04_MH_Breach',
																				IF(
																					NOT(isnull(shipment_first_consignment_create_datetime)) AND from_unixtime(unix_timestamp(shipment_first_consignment_create_datetime)) > from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)+(120 * 60)),'04_MH_Breach',
																						IF(
																							isnull(fsd_assignedhub_received_time) AND from_unixtime(unix_timestamp())> from_unixtime(unix_timestamp(fsd_assigned_hub_expected_receive_datetime)),'05_LH_Breach',
																								IF(
																									NOT(isnull(fsd_assignedhub_received_time)) AND from_unixtime(unix_timestamp(fsd_assignedhub_received_time)) > from_unixtime(unix_timestamp(fsd_assigned_hub_expected_receive_datetime)),'05_LH_Breach','06_LM_Breach'
																								)
																						)
																				)
																		)
																)
														)
												)
										)
								)				
						)
				)
		)		
) AS breach_bucket,
shipment_rto_create_time,
cust_dependency_reason,
first_ofd_runsheet_id,
first_ofd_agent_id_key,
order_delivered_flag, 
runsheet_shipment_delivered_count,
runsheet_shipment_total_count
from
(select
oms.order_item_unit_quantity, oms.order_item_quantity, oms.order_item_selling_price, oms.order_item_type, oms.order_item_title, oms.order_item_unit_status, oms.order_item_unit_is_promised_date_updated, oms.order_item_sub_type, oms.order_id, oms.order_item_id, oms.order_item_unit_tracking_id, oms.order_item_sku, oms.order_item_category_id, oms.order_item_sub_status, oms.order_item_ship_group_id, oms.order_item_status, oms.order_item_unit_shipment_id, oms.order_item_unit_id, oms.order_item_unit_init_promised_datetime, oms.order_item_product_id, oms.order_item_product_id_key, oms.order_item_unit_seller_id, oms.order_item_unit_source_facility, oms.order_item_listing_id, oms.order_item_date, oms.order_item_date_key, oms.order_item_created_at, oms.order_item_unit_final_promised_date_use, oms.order_item_unit_final_promised_date_key, oms.order_item_last_update, oms.order_item_unit_new_promised_date, oms.order_shipping_address_id, oms.order_shipping_address_id_key, oms.order_external_id, oms.account_id, oms.order_item_max_approved_time, oms.order_item_max_on_hold_time, oms.tracking_id_type, oms.customer_promise_proactive_age, 
ff.fulfill_item_unit_dispatch_actual_datetime, ff.fulfill_item_unit_dispatch_actual_date_key, ff.fulfill_item_unit_ship_actual_time, ff.fulfill_item_unit_dispatch_expected_datetime, ff.fulfill_item_unit_dispatch_expected_date_key, ff.fulfill_item_unit_ship_expected_time, ff.fulfillment_order_item_id, ff.fulfill_item_unit_id, ff.fulfill_item_unit_status, ff.shipment_merchant_reference_id, ff.fulfill_item_unit_updated_at, ff.fulfill_item_unit_region, ff.fulfill_item_unit_region_type, ff.fulfill_item_id, ff.fulfill_item_type, ff.fulfill_item_unit_deliver_after_time, ff.fulfill_item_unit_dispatch_after_time, ff.fulfill_item_unit_is_for_slotted_delivery, ff.fulfill_item_unit_reserved_status_b2c_actual_time, ff.fulfill_item_unit_reserved_status_expected_time, ff.fulfill_item_unit_delivered_status_expected_time, ff.fulfill_item_unit_delivered_status_id, ff.fulfill_item_unit_size, ff.fulfill_item_unit_reserved_status_b2b_actual_time, ff.slot_changed_created_at, ff.slot_changed_by,ff.slot_booking_ref_id, ff.customer_destination_pincode, ff.fc_breach_flag, ff.fc_pending_flag, ff.fc_pending_age, 
sh.shipment_id, sh.vendor_tracking_id, sh.shipment_current_status, sh.shipment_current_status_datetime, sh.payment_type, sh.destination_pincode, sh.ekl_shipment_type, sh.customer_promise_datetime, sh.logistics_promise_datetime, sh.shipment_created_at_datetime, sh.shipment_created_at_date_key, sh.vendor_id, sh.vendor_id_key, sh.shipment_carrier, sh.fsd_assigned_hub_id, sh.fsd_assigned_hub_id_key, sh.fsd_last_current_hub_id, sh.fsd_last_current_hub_id_key, sh.fsd_last_current_hub_type, sh.ekl_billable_weight, sh.merchant_reference_id, sh.shipment_first_consignment_id, sh.shipment_last_consignment_id, sh.shipment_inscan_time, sh.fsd_last_received_time, sh.fsd_number_of_ofd_attempts, sh.fsd_assignedhub_expected_time, sh.fsd_assignedhub_received_time, sh.ekl_delivery_datetime, sh.ekl_delivery_date_key, sh.shipment_first_ofd_datetime, sh.shipment_last_ofd_datetime, sh.vendor_dispatch_datetime, sh.tpl_first_ofd_time, sh.tpl_last_ofd_time, sh.fsd_assigned_hub_sent_datetime, sh.openbox_reject_datetime, sh.merchant_id, sh.fsd_first_dh_received_datetime, sh.shipment_value, sh.runsheet_close_datetime, sh.COD_amount_to_collect, sh.shipment_first_consignment_create_datetime, sh.shipment_last_consignment_create_datetime, sh.shipment_origin_facility_id, sh.end_state_datetime, sh.shipment_first_consignment_conn_id, sh.shipment_last_consignment_conn_id, sh.shipment_first_consignment_eta_in_sec, sh.shipment_last_consignment_eta_in_sec, sh.fsd_current_customer_dependency_flag, sh.shipment_dh_pending_flag, sh.ekl_pending_flag, sh.openbox_reject_flag, sh.shipment_dh_days_for_delivery, sh.first_attempt_delivery_flag,sh.shipment_rto_create_time, 
st.slot_id, st.slot_code, st.slot_facility_id, st.slot_facility_id_key, st.slot_start_date, st.slot_start_date_key, st.slot_end_date, st.slot_actual_capacity, st.slot_shipment_size, st.slot_duration, st.slot_booking_id, st.slot_reservation_status, 
prod.product_categorization_hive_dim_key, prod.order_item_product_title, prod.analytic_super_category, prod.order_item_brand, prod.analytic_vertical, prod.analytic_category, 
facility_1.slot_facility_name,
facility_1.slot_facility_zone,
facility_1.slot_facility_type,
facility_2.fsd_last_current_hub_name,
facility_3.fsd_assigned_hub_name,
facility_3.fsd_assigned_hub_zone,
facility_3.fsd_assigned_hub_type,
vendors.shipment_vendor_name,
to_date(from_unixtime(unix_timestamp())) as present_day,
from_unixtime(unix_timestamp()) as present_time,
If(isnull(fulfill_item_unit_dispatch_actual_datetime) and (from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime),'yyyy-MM-dd')>=from_unixtime(unix_timestamp(),'yyyy-MM-dd')),'In-Progress',if(fulfill_item_unit_dispatch_expected_datetime>=fulfill_item_unit_dispatch_actual_datetime,'FC_fulfilled',
if(from_unixtime(unix_timestamp(oms.order_item_max_on_hold_time)+(5*60*30))>from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)),'FC_CS_Breach',
if(from_unixtime(unix_timestamp(fulfill_item_unit_reserved_status_b2c_actual_time)+(5*60*30))>from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime)),'FC_Fulfilment_Tech_Breach',
'FC_Operations_Breach')))) as fc_breach_bucket,
 if(oms.tracking_id_type='forward', 
 if(fulfill_item_unit_is_for_slotted_delivery='Slotted',to_date(order_item_unit_final_promised_date_use),date_add(to_date(fsd_assignedhub_received_time),1)),NULL)
 as shipment_dh_fwd_promised_date, 
 if(ekl_pending_flag='not_pending','not_pending', 
if(ekl_shipment_type='rvp' and shipment_carrier='FSD','RVP_shipment',
if(shipment_carrier='VNF','VNF',
if(shipment_dh_pending_flag=1,'DH_pending',
if(shipment_current_status='Out_For_Delivery','Out_For_Delivery',
if(shipment_current_status='Expected','Intransit',
if(ekl_shipment_type='forward',
if(isnull(shipment_inscan_time),'MH_inscan_pending',
if(shipment_carrier='3PL',
if(shipment_current_status='received','3PL_Central_Hub_Pending','3PL_pending'),
if(isnull(shipment_first_consignment_id),'consignment_creation_pending',fsd_last_current_hub_type)
)), if(shipment_current_status in('Received_By_Ekl','Returned_To_Ekl'),'Central_Hub_Pending',
if(shipment_carrier='3PL',
if(isnull(vendor_dispatch_datetime),'3PL_Central_Hub_Pending','3PL_pending'),
fsd_last_current_hub_type))))))))) as shipment_pending_location,
if(shipment_carrier='FSD',
 if(to_date(ekl_delivery_datetime)<=to_date(order_item_unit_final_promised_date_use), 0,1),
 if(to_date(ekl_delivery_datetime)<=to_date(order_item_unit_final_promised_date_use), 0,1) ) as shipment_breach_flag,
  if(ekl_shipment_type='forward' and isnull(ekl_delivery_datetime), 
 datediff(to_date(from_unixtime(unix_timestamp())), to_Date(order_item_unit_final_promised_date_use)),NULL) as pending_beyond_customer_promise_age,
(CASE
WHEN order_item_status in ('returned','return_requested') or ekl_shipment_type in ('approved_rto','unapproved_rto')
THEN '12_Returned'
WHEN order_item_status in ('delivered') or shipment_current_status in ('delivered','Delivered') or ekl_delivery_datetime is not null
THEN '11_Delivered'
WHEN order_item_status in ('cancelled') or shipment_current_status in ('cancelled')
THEN '13_Cancelled'
WHEN order_item_status in ('created','on_hold')
THEN '01_OMS'
WHEN isnull(fulfill_item_unit_reserved_status_b2c_actual_time) and isnull(fulfill_item_unit_reserved_status_b2b_actual_time) and isnull(shipment_created_at_datetime)
THEN '02_Fulfillment'
WHEN isnull(fulfill_item_unit_dispatch_actual_datetime) and isnull(shipment_created_at_datetime)
THEN '03_Warehouse'
WHEN shipment_carrier='3PL'
THEN '10_3PL'
WHEN isnull(shipment_first_consignment_create_datetime)
THEN '04_MotherHub'
WHEN isnull(fsd_assignedhub_received_time)
THEN '05_Transport'
WHEN isnull(shipment_first_ofd_datetime) and isnull(tpl_first_ofd_time)
THEN '06_DeliveryHub'
ELSE '00_Catch_in_system_error'
END) as asset,
ff.mh_cutoff_datetime,
lookupkey('pincode',if(isnull(cust_add.cust_add_pincode),ff.customer_destination_pincode,cust_add.cust_add_pincode)) as customer_destination_pincode_key,
hub.pincode_hub as calculated_delivery_hub,
hub.pincode_hub_zone as calculated_delivery_hub_zone,
sh.shipment_origin_facility_id_key,
ff.fulfill_item_unit_final_reserved_datetime,
lookup_date(ff.fulfill_item_unit_final_reserved_datetime) as fulfill_item_unit_final_reserved_date_key,
CASE 
WHEN fulfill_item_unit_is_for_slotted_delivery='NotSlotted' THEN to_utc_timestamp(concat(substr(order_item_unit_final_promised_date_key,1,4),'-',substr(order_item_unit_final_promised_date_key,5,2),'-',substr(order_item_unit_final_promised_date_key,7,2),' 06:00:00'),'')
WHEN fulfill_item_unit_is_for_slotted_delivery='Slotted' 
THEN 
to_utc_timestamp(from_unixtime(IF(unix_timestamp(shipment_last_consignment_eta_datetime) >=
unix_timestamp(fulfill_item_unit_dispatch_expected_datetime) + 7200 + unix_timestamp(shipment_last_consignment_eta_datetime) - unix_timestamp(shipment_first_consignment_create_datetime),
unix_timestamp(shipment_last_consignment_eta_datetime),
unix_timestamp(fulfill_item_unit_dispatch_expected_datetime) + 7200 + unix_timestamp(shipment_last_consignment_eta_datetime) - unix_timestamp(shipment_first_consignment_create_datetime)
)),'')
END AS fsd_assigned_hub_expected_receive_datetime,
first_conn.first_conn_cutoff_in_sec,
last_conn.last_conn_cutoff_in_sec,
if(isnull(shipment_last_consignment_create_datetime),0,
if((last_conn.last_conn_cutoff_in_sec - HOUR(shipment_last_consignment_create_datetime)*3600 - MINUTE(shipment_last_consignment_create_datetime)*60 - 
SECOND(shipment_last_consignment_create_datetime))>0,(last_conn.last_conn_cutoff_in_sec - HOUR(shipment_last_consignment_create_datetime)*3600 - 
MINUTE(shipment_last_consignment_create_datetime)*60 - SECOND(shipment_last_consignment_create_datetime)),0)) as tpt_time_diff_in_sec,
cust_add.cust_add_pincode as customer_address_pincode,
cust_hub.customer_pincode_hub,
cust_hub.customer_pincode_hub_zone,
cust_hub.customer_pincode_route,
if(assoc.order_item_is_replacement>0,1,0) as is_replacement,
if(assoc.order_item_is_duplicate>0,1,0) as is_duplicate,
if(assoc.order_item_is_exchange>0,1,0) as is_exchange,
(CASE 
WHEN oms_pay.order_last_payment_method = 'COD'
THEN 'COD'
ELSE 'Prepaid'
END) AS order_payment_type,
on_hold.onhold_reason as onhold_reason,
on_hold.order_item_status_change_sub_reason as onhold_sub_reason,
cd.customer_dependency_flag as customer_dependency_flag,
cd.cust_dependency_reason as cust_dependency_reason,
ofd_runsheet.tasklist_id as first_ofd_runsheet_id,
ofd_runsheet.primary_agent_id as first_ofd_agent_id_key,
ofd_runsheet.order_delivered_flag as order_delivered_flag, 
runsheet_data.runsheet_delivered_count as runsheet_shipment_delivered_count,
runsheet_data.runsheet_total_count as runsheet_shipment_total_count
from
(select 
order_item_unit_quantity, order_item_quantity, order_item_selling_price, order_item_type, order_item_title, order_item_unit_status, order_item_unit_is_promised_date_updated, order_item_sub_type, order_id, order_item_id, order_item_unit_tracking_id, order_item_sku, order_item_category_id, order_item_sub_status, order_item_ship_group_id, order_item_status, order_item_unit_shipment_id, order_item_unit_id, order_item_unit_init_promised_datetime, order_item_product_id, order_item_unit_seller_id, order_item_unit_source_facility, order_item_listing_id, order_item_date, order_item_created_at, order_item_unit_final_promised_date_use, order_item_last_update, order_item_unit_new_promised_date, order_shipping_address_id, order_external_id, account_id, order_item_max_approved_time, order_item_max_on_hold_time, tracking_id_type, 
lookupkey('product_id',order_item_product_id) as order_item_product_id_key,
lookup_date(order_item_date) AS order_item_date_key,
lookup_date(order_item_unit_final_promised_date_use) as order_item_unit_final_promised_date_key,
lookupkey('address_id',order_shipping_address_id) as order_shipping_address_id_key,
datediff(to_date(order_item_unit_final_promised_date_use), to_date(from_unixtime(unix_timestamp())))  as customer_promise_proactive_age
from bigfoot_external_neo.scp_oms__la_14d_oms_fact) oms
left outer join
(SELECT 
`data`.payment_ref_num_2 AS order_external_id, 
first_value(`data`.payment_method) over (
partition by `data`.payment_ref_num_2
ORDER BY `data`.updated_at  DESC) 
AS order_last_payment_method
FROM bigfoot_snapshot.dart_fkint_apl_finance_reporting_payment_1_2_view
WHERE 
`data`.bu_id='FKMP_CUSTOMER' 
AND `data`.type IN ('CustomerPayment','CustomerCredit')
AND ((`data`.payment_ref_num_3 IS NOT NULL AND `data`.payment_method="COD")
OR (`data`.status = "received" AND `data`.payment_method<>"COD" and `data`.payment_method<>"POS"))
)  oms_pay on oms_pay.order_external_id=oms.order_external_id
left outer join
(SELECT `data`.order_item_assoc_to_order_item as order_item_assoc_to_order_item,
sum(if(`data`.order_item_assoc_type = 'replacement',1,0)) as order_item_is_replacement,
sum(if(`data`.order_item_assoc_type = 'duplicate',1,0)) as order_item_is_duplicate,
sum(if(`data`.order_item_assoc_type = 'exchange',1,0)) as order_item_is_exchange
from bigfoot_snapshot.dart_fkint_scp_oms_order_item_assoc_1_view group by `data`.order_item_assoc_to_order_item) assoc on (assoc.order_item_assoc_to_order_item = oms.order_item_id)
left outer join
(select stat.order_item_status_order_item as order_item_status_order_item ,stat.order_item_status_change_sub_reason as order_item_status_change_sub_reason,stat.order_item_status_change_time as order_item_status_change_time,stat.onhold_reason as onhold_reason,stat.row_n from
 (SELECT `data`.order_item_status_order_item as order_item_status_order_item ,`data`.order_item_status_change_reason as order_item_status_change_reason,`data`.order_item_status_change_time as order_item_status_change_time,`data`.order_item_status_change_sub_reason,
FIRST_VALUE(`data`.order_item_status_change_reason) OVER (PARTITION BY `data`.order_item_status_order_item ORDER BY `data`.order_item_status_change_time desc) AS onhold_reason,row_number() Over ( PARTITION BY `data`.order_item_status_order_item ORDER BY `data`.order_item_status_change_time desc ) as row_n
FROM  bigfoot_journal.dart_fkint_scp_oms_order_item_status_change_1_0_view where `data`.order_item_status_to = 'on_hold' )stat
where stat.row_n=1)on_hold
on on_hold.order_item_status_order_item=oms.order_item_id
left outer join
(select
fulfill_item_unit_dispatch_actual_datetime, fulfill_item_unit_ship_actual_time, fulfill_item_unit_dispatch_expected_datetime, fulfill_item_unit_ship_expected_time, fulfillment_order_item_id, fulfill_item_unit_id, fulfill_item_unit_status, shipment_merchant_reference_id, fulfill_item_unit_updated_at, fulfill_item_unit_region, fulfill_item_unit_region_type, fulfill_item_id, fulfill_item_type, fulfill_item_unit_deliver_after_time, fulfill_item_unit_dispatch_after_time, fulfill_item_unit_is_for_slotted_delivery, fulfill_item_unit_reserved_status_b2c_actual_time, fulfill_item_unit_reserved_status_expected_time, fulfill_item_unit_delivered_status_expected_time, fulfill_item_unit_delivered_status_id, fulfill_item_unit_size, fulfill_item_unit_reserved_status_b2b_actual_time, slot_changed_created_at, slot_changed_by, slot_booking_ref_id, customer_destination_pincode, 
lookup_date(fulfill_item_unit_dispatch_actual_datetime) as fulfill_item_unit_dispatch_actual_date_key,
lookup_date(fulfill_item_unit_dispatch_expected_datetime) as fulfill_item_unit_dispatch_expected_date_key,
If(isnull(fulfill_item_unit_dispatch_actual_datetime),
If(from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime),'yyyy-MM-dd')<from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1,0),
if(fulfill_item_unit_dispatch_expected_datetime<fulfill_item_unit_dispatch_actual_datetime,1,0)) as fc_breach_flag,
if(isnull(fulfill_item_unit_dispatch_actual_datetime),
 If(from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime),'yyyy-MM-dd')<from_unixtime(unix_timestamp(),'yyyy-MM-dd'),
 'pending_without_breach','pending_with_breach'),'not_pending') as fc_pending_flag,
If(from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime),'yyyy-MM-dd')<from_unixtime(unix_timestamp(),'yyyy-MM-dd'),
if(isnull(fulfill_item_unit_dispatch_actual_datetime),datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 
from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_datetime),'yyyy-MM-dd'))-1,Null),Null) as  fc_pending_age,
from_unixtime(unix_timestamp(if(fulfill_item_unit_dispatch_expected_datetime>fulfill_item_unit_dispatch_actual_datetime or
 isnull(fulfill_item_unit_dispatch_actual_datetime),
fulfill_item_unit_dispatch_expected_datetime,fulfill_item_unit_dispatch_actual_datetime)) + 7200) 
as mh_cutoff_datetime,
if(isnull(fulfill_item_unit_reserved_status_b2b_actual_time),
if(isnull(fulfill_item_unit_reserved_status_b2c_actual_time),null,fulfill_item_unit_reserved_status_b2c_actual_time),
if(isnull(fulfill_item_unit_reserved_status_b2c_actual_time),fulfill_item_unit_reserved_status_b2b_actual_time,
if(fulfill_item_unit_reserved_status_b2b_actual_time<fulfill_item_unit_reserved_status_b2c_actual_time,fulfill_item_unit_reserved_status_b2b_actual_time,
fulfill_item_unit_reserved_status_b2c_actual_time))) as fulfill_item_unit_final_reserved_datetime
from bigfoot_external_neo.scp_ekl__la_14d_fulfillment_fact) ff
on (oms.order_item_id=ff.fulfillment_order_item_id
AND oms.order_item_unit_shipment_id <=> ff.shipment_merchant_reference_id)
left outer join
(select
shipment_id, vendor_tracking_id, shipment_current_status, shipment_current_status_datetime, payment_type, destination_pincode, ekl_shipment_type, customer_promise_datetime, logistics_promise_datetime, shipment_created_at_datetime, vendor_id, shipment_carrier, fsd_assigned_hub_id, fsd_last_current_hub_id, fsd_last_current_hub_type, ekl_billable_weight, merchant_reference_id, shipment_first_consignment_id, shipment_last_consignment_id, shipment_inscan_time, fsd_last_received_time, fsd_number_of_ofd_attempts, fsd_assignedhub_expected_time, fsd_assignedhub_received_time, ekl_delivery_datetime, shipment_first_ofd_datetime, shipment_last_ofd_datetime, vendor_dispatch_datetime, tpl_first_ofd_time, tpl_last_ofd_time, fsd_assigned_hub_sent_datetime, openbox_reject_datetime, merchant_id, fsd_first_dh_received_datetime, shipment_value, runsheet_close_datetime, COD_amount_to_collect, shipment_first_consignment_create_datetime, shipment_last_consignment_create_datetime, shipment_origin_facility_id, end_state_datetime, shipment_first_consignment_conn_id, shipment_last_consignment_conn_id, shipment_first_consignment_eta_in_sec, shipment_last_consignment_eta_in_sec, shipment_last_consignment_eta_datetime,shipment_rto_create_time,
lookup_date(shipment_created_at_datetime) as shipment_created_at_date_key,
lookupkey('vendor_id',vendor_id) as vendor_id_key,
lookupkey('facility_id',fsd_assigned_hub_id) as fsd_assigned_hub_id_key,
lookupkey('facility_id',fsd_last_current_hub_id) as fsd_last_current_hub_id_key,
lookupkey('facility_id',shipment_origin_facility_id) as shipment_origin_facility_id_key,
lookup_date(ekl_delivery_datetime) as ekl_delivery_date_key,
if(shipment_current_status in ('Undelivered_Attempted','Undelivered_COD_Not_Ready','Undelivered_Customer_Not_Available',
'Undelivered_Door_Lock','Undelivered_Incomplete_Address','Undelivered_No_Response','Undelivered_NonServiceablePincode',
'Undelivered_Order_Rejected_By_Customer','Undelivered_Order_Rejected_OpenDelivery','Undelivered_OutOfDeliveryArea',
'Undelivered_Request_For_Reschedule'),1,0) as fsd_current_customer_dependency_flag,
if (fsd_last_current_hub_type in ('BULK_HUB','DELIVERY_HUB') and shipment_current_status<>'Expected',1,0) as shipment_dh_pending_flag, 
if(not(isnull(ekl_delivery_datetime)) or not isnull(end_state_datetime) or shipment_current_status 
 in('not_received','reverse_pickup_cancelled','PICKUP_Cancelled','NotPicked_Attempted_CustomerHappyWithProduct',
 'expected','dispatch_failed','Delivered','delivered', 'reshipped','delivered','returned_to_seller',
 'received_by_merchant', 'damaged','returned','Undelivered_UntraceableFromHub','Not_Received'),'not_pending','pending') as ekl_pending_flag,
 if(isnull(openbox_reject_datetime),0,1) as openbox_reject_flag,
 if(ekl_shipment_type='forward' and not isnull(ekl_delivery_datetime) and not isnull(fsd_assignedhub_received_time), 
datediff(to_date(ekl_delivery_datetime), to_date(fsd_assignedhub_received_time)),NULL ) as shipment_dh_days_for_delivery,
if(ekl_shipment_type='forward',if(not isnull(ekl_delivery_datetime) and fsd_number_of_ofd_attempts=1,1,0),NULL) as first_attempt_delivery_flag
from bigfoot_external_neo.scp_ekl__la_14d_shipment_fact) sh
on oms.order_item_unit_shipment_id=sh.merchant_reference_id
left outer join
(
Select slot.slot_id, slot_code, slot_facility_id, slot_start_date, slot_end_date, slot_actual_capacity, slot_shipment_size, slot_duration, slot.slot_booking_id, slot_reservation_status, 
vendor_tracking_id,
lookupkey('facility_id',slot_facility_id) as slot_facility_id_key,
lookup_date(slot_start_date) as slot_start_date_key
FROM (
select slot_booking_id,max(slot_id)as slot_id from bigfoot_external_neo.scp_ekl__la_14d_slot_fact group by slot_booking_id) slot
JOIN bigfoot_external_neo.scp_ekl__la_14d_slot_fact slot1
ON slot.slot_id = slot1.slot_id
AND slot.slot_booking_id = slot1.slot_booking_id) st
on ff.slot_booking_ref_id=st.slot_booking_id
and sh.vendor_tracking_id = st.vendor_tracking_id
left outer join
(select product_categorization_hive_dim_key,title as order_item_product_title, analytic_super_category, brand as order_item_brand,analytic_vertical, analytic_category
from bigfoot_external_neo.sp_product__product_categorization_hive_dim) prod
on prod.product_categorization_hive_dim_key=oms.order_item_product_id_key
left outer join
(select
ekl_hive_facility_dim_key, name as slot_facility_name, zone as slot_facility_zone, type as slot_facility_type
from bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim) facility_1
on facility_1.ekl_hive_facility_dim_key=st.slot_facility_id_key
left outer join
(select
ekl_hive_facility_dim_key, name as fsd_last_current_hub_name, zone as fsd_last_current_hub_zone
from bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim) facility_2
on facility_2.ekl_hive_facility_dim_key=sh.fsd_last_current_hub_id_key
left outer join
(select
ekl_hive_facility_dim_key, name as fsd_assigned_hub_name, zone as fsd_assigned_hub_zone, type as fsd_assigned_hub_type
from bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim) facility_3
on facility_3.ekl_hive_facility_dim_key=sh.fsd_assigned_hub_id_key
left outer join
(select logistics_vendor_hive_dim_key, vendor_name as shipment_vendor_name, carrier as shipment_carrier_2
from bigfoot_external_neo.scp_ekl__logistics_vendor_hive_dim) vendors
on vendors.logistics_vendor_hive_dim_key=sh.vendor_id_key
left outer join
(select distinct
pincode as pin_codes,
route  as pincode_route,
hub_name as pincode_hub,
hub_zone as pincode_hub_zone
from bigfoot_common.large_pincode_hub_mapping) hub
on hub.pin_codes=ff.customer_destination_pincode
left outer join
(select distinct
`data`.group_id as first_group_id,
`data`.cutoff as first_conn_cutoff_in_sec
from bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total ) first_conn
on first_conn.first_group_id=sh.shipment_first_consignment_conn_id
left outer join
(select distinct
`data`.group_id as last_group_id,
`data`.cutoff as last_conn_cutoff_in_sec
from bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total ) last_conn
on last_conn.last_group_id=sh.shipment_last_consignment_conn_id
left outer join
(select entityid as cust_add_entity_id,
`data`.useraddress.pincode as cust_add_pincode,
`data`.useraddress.addressid as cust_add_id
from bigfoot_snapshot.dart_fkint_cp_user_contact_2_0_view_total) cust_add
on oms.order_shipping_address_id=cust_add.cust_add_id
left outer join
(select distinct
pincode as customer_address_pincode,
route  as customer_pincode_route,
hub_name as customer_pincode_hub,
hub_zone as customer_pincode_hub_zone
from bigfoot_common.large_pincode_hub_mapping) cust_hub
on cust_hub.customer_address_pincode=cust_add.cust_add_pincode
left outer join bigfoot_external_neo.scp_ekl__customer_dependency_14_fact cd on cd.vendor_tracking_id = sh.vendor_tracking_id
left outer join 
(select distinct
vendor_tracking_id,
tasklist_id,
primary_agent_id,
shipment_actioned_flag as order_delivered_flag 
from bigfoot_external_neo.scp_ekl__runsheet_shipment_map_l1_fact
where attempt_no =1
) ofd_runsheet 
on ofd_runsheet.vendor_tracking_id = sh.vendor_tracking_id
left outer join 
(select 
tasklist_id,
sum(shipment_actioned_flag) as runsheet_delivered_count,
count(distinct vendor_tracking_id) as runsheet_total_count
from bigfoot_external_neo.scp_ekl__runsheet_shipment_map_l1_fact
group by tasklist_id
) runsheet_data 
on runsheet_data.tasklist_id = ofd_runsheet.tasklist_id 
)A;