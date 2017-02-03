Insert overwrite table la_14d_fulfillment_fact
select distinct
fiu.fulfill_item_unit_dispatch_actual_datetime,
fiu.fulfill_item_unit_ship_actual_time,
fiu.fulfill_item_unit_dispatch_expected_datetime,
fiu.fulfill_item_unit_ship_expected_time,
fiu.fulfillment_order_item_id,
fiu.fulfill_item_unit_id,
fiu.fulfill_item_unit_status,
fiu.shipment_merchant_reference_id,
fiu.fulfill_item_unit_updated_at,
fiu.fulfill_item_unit_region,
fiu.fulfill_item_unit_region_type,
fiu.fulfill_item_id,
fiu.fulfill_item_type,
fiu.fulfill_item_unit_deliver_after_time,
fiu.fulfill_item_unit_dispatch_after_time,
fiu.fulfill_item_unit_is_for_slotted_delivery,
fiu.fulfill_item_unit_reserved_status_b2c_actual_time,
fiu.fulfill_item_unit_reserved_status_expected_time,
fiu.fulfill_item_unit_delivered_status_expected_time,
fiu.fulfill_item_unit_delivered_status_expected_date_key,
fiu.fulfill_item_unit_delivered_status_id,
fiu.fulfill_item_unit_size,
fiu.fulfill_item_unit_reserved_status_b2b_actual_time,
fiu_sc.slot_changed_created_at,
if(slot_changed_created_at is not null, if(fiu_sc.slot_change_reason like '%Customer%', 'cust', 'ekl'), 'slot_unchanged') as slot_changed_by,
ffd.slot_booking_ref_id,
ffd.customer_destination_pincode
from 
(SELECT DISTINCT 
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time as fulfill_item_unit_dispatch_actual_datetime,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time as fulfill_item_unit_ship_actual_time,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time as fulfill_item_unit_dispatch_expected_datetime,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_expected_time as fulfill_item_unit_ship_expected_time,
`data`.fulfill_item_unit_order_item_mapping.order_item_mapping_external_id as fulfillment_order_item_id,
`data`.fulfill_item_unit_id as fulfill_item_unit_id,
`data`.fulfill_item_unit_status as fulfill_item_unit_status,
`data`.fulfill_item_unit_ekl_shipment_mapping.ekl_shipment_mapping_external_id as shipment_merchant_reference_id,
`data`.fulfill_item_unit_updated_at as fulfill_item_unit_updated_at,
`data`.fulfill_item_unit_region as fulfill_item_unit_region,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_order_date as fulfill_item_unit_order_date,
`data`.fulfill_item_unit_region_type as fulfill_item_unit_region_type,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_id as fulfill_item_id,
lookupkey('product_id',`data`.fulfill_item_unit_fulfill_item.fulfill_item_fsn) as fulfill_item_product_id_key,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_type as fulfill_item_type,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time as fulfill_item_unit_deliver_after_time,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_after_time as fulfill_item_unit_dispatch_after_time,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time as fulfill_item_unit_deliver_after_datetime,
case when `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time is null then 'NotSlotted' else 'Slotted' end  as fulfill_item_unit_is_for_slotted_delivery,
`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time as fulfill_item_unit_reserved_status_b2c_actual_time,
`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time as fulfill_item_unit_reserved_status_expected_time,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time as fulfill_item_unit_delivered_status_expected_time,
lookup_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) as fulfill_item_unit_delivered_status_expected_date_key,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_id as fulfill_item_unit_delivered_status_id,
 `data`.Fulfill_Item_Unit_Size as fulfill_item_unit_size,
`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time as fulfill_item_unit_reserved_status_b2b_actual_time
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_2_view_14d  A_22_1 where `data`.Fulfill_Item_Unit_Size in ('Large')) fiu
left outer join 
(select fiu_status_id,
slot_changed_created_at,
slot_change_reason,
row_desc from 
(select
`data`.fulfill_item_unit_status_id as fiu_status_id,
`data`.created_at as slot_changed_created_at,
LAST_VALUE(`data`.reason) OVER (PARTITION BY `data`.fulfill_item_unit_status_id ORDER BY `data`.created_at rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS slot_change_reason,
row_number() OVER (PARTITION BY `data`.fulfill_item_unit_status_id ORDER BY `data`.created_at DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as row_desc
FROM bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1_view_14d
where `data`.reason like '%slot%' or `data`.reason like '%SLOT%' or `data`.reason like '%Delayed%' or `data`.reason like '%DELAYED%' or `data`.reason like '%/RESCHEDULED%'
) fiu_status_change 
where fiu_status_change.row_desc = '1') fiu_sc     
ON (fiu_sc.fiu_status_id = fiu.fulfill_item_unit_delivered_status_id)
left outer join
(select
`data`.fulfillment_done_fulfill_item_unit_id as ffd_fulfill_item_unit_id,
`data`.fulfillment_done_fulfill_item_id as ffd_fulfill_item_id,
`data`.fulfillment_done_booking_ref_id as slot_booking_ref_id,
`data`.fulfillment_done_destination_pincode as customer_destination_pincode
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfillment_done_6_view
where `data`.fulfillment_done_destination_pincode is not null) ffd
on fiu.fulfill_item_unit_id=ffd.ffd_fulfill_item_unit_id
where not(isnull(fulfill_item_unit_id));