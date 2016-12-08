Insert overwrite table la_14d_shipment_fact
select distinct
sv.shipment_id,
sv.vendor_tracking_id,
sv.shipment_current_status,
sv.shipment_current_status_datetime,
sv.payment_type,
sv.destination_pincode,
sv.ekl_shipment_type,
sv.customer_promise_datetime,
sv.logistics_promise_datetime,
sv.shipment_created_at_datetime,
sv.vendor_id,
sv.shipment_carrier,
sv.fsd_assigned_hub_id,
sv.fsd_last_current_hub_id,
sv.fsd_last_current_hub_type,
sv.ekl_billable_weight,
slog.merchant_reference_id,
sc.shipment_first_consignment_id,
sc.shipment_last_consignment_id,
sreh.shipment_inscan_time,
sreh.fsd_last_received_time,
sreh.fsd_number_of_ofd_attempts,
sreh.fsd_assignedhub_expected_time,
sreh.fsd_assignedhub_received_time,
sreh.ekl_delivery_datetime,
sreh.shipment_first_ofd_datetime,
sreh.shipment_last_ofd_datetime,
sreh.vendor_dispatch_datetime,
sreh.tpl_first_ofd_time,
sreh.tpl_last_ofd_time,
sreh.fsd_assigned_hub_sent_datetime,
sreh.openbox_reject_datetime,
slog.merchant_id,
sreh.fsd_first_DH_received_datetime,
sreh.shipment_value,
sreh.runsheet_close_datetime,
sreh.COD_amount_to_collect,
sc.shipment_first_consignment_create_datetime,
sc.shipment_last_consignment_create_datetime,
sv.shipment_origin_facility_id,
sreh.end_state_datetime,
sc.shipment_first_consignment_conn_id,
sc.shipment_last_consignment_conn_id,
sc.shipment_first_consignment_eta_in_sec,
sc.shipment_last_consignment_eta_in_sec
from 
( Select is_large,product_categorization_hive_dim_key  
from bigfoot_external_neo.sp_product__product_categorization_hive_dim where is_large=1) prod
left outer join
(select distinct
entityid as shipment_id,
`data`.vendor_tracking_id as vendor_tracking_id,
`data`.status as shipment_current_status,
cast(updatedat/1000 as TIMESTAMP) as shipment_current_status_datetime,
`data`.payment_type as payment_type,
`data`.destination_address.pincode as destination_pincode,
`data`.shipment_type as ekl_shipment_type,
`data`.customer_sla AS customer_promise_datetime,
`data`.design_sla AS logistics_promise_datetime,
`data`.created_at as shipment_created_at_datetime,
`data`.vendor_id,
If(`data`.vendor_id = '','VNF',If(`data`.vendor_id = 200 OR `data`.vendor_id =207, 'FSD', '3PL')) as shipment_carrier,
`data`.assigned_address.id as fsd_assigned_hub_id,
`data`.current_address.id as fsd_last_current_hub_id,
`data`.current_address.type as fsd_last_current_hub_type,
`data`.billable_weight as ekl_billable_weight,
`data`.source_address.id as shipment_origin_facility_id,
shipment_items.product_id as shipment_product_id,
lookupkey('product_id',shipment_items.product_id) as shipment_product_id_key
from
bigfoot_journal.dart_wsr_scp_ekl_shipment_4_view_14d lateral view explode(`data`.shipment_items) exploded_table AS shipment_items) sv
on prod.product_categorization_hive_dim_key=sv.shipment_product_id_key
left outer join
(SELECT DISTINCT refid AS shipment_id,`data`.merchant_id AS merchant_id,`data`.merchant_reference_id AS merchant_reference_id FROM
 bigfoot_journal.dart_wsr_scp_ekl_b2clogisticsrequest_1_view_14d LATERAL VIEW explode(`data`.ekl_reference_ids) reference_id AS refid) slog 
on  sv.shipment_id=slog.shipment_id
left outer join 
(Select 
C.shipment_first_consignment_id as shipment_first_consignment_id,
C.shipment_last_consignment_id as shipment_last_consignment_id,
from_unixtime(shipment_first_consignment_create_datetime) as shipment_first_consignment_create_datetime,
from_unixtime(shipment_last_consignment_create_datetime) as shipment_last_consignment_create_datetime,
shipment_first_consignment_conn_id,
shipment_last_consignment_conn_id,
shipment_first_consignment_eta_in_sec,
shipment_last_consignment_eta_in_sec,
C.tracking_id as vendor_tracking_id
From
(Select 
B.shipment_first_consignment_id,
B.shipment_first_consignment_create_datetime,
B.shipment_first_consignment_conn_id,
B.shipment_first_consignment_eta_in_sec,
B.row_n,
B.shipment_last_consignment_id,
B.shipment_last_consignment_create_datetime,
B.shipment_last_consignment_conn_id,
B.shipment_last_consignment_eta_in_sec,
B.l_row_n,
B.tracking_id from
(Select 
first_value(A.consignment_id) Over (PARTITION By A.tracking_id order by A.first_time ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_first_consignment_id,
first_value(A.first_time) Over (PARTITION By A.tracking_id order by A.first_time ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_first_consignment_create_datetime,
first_value(A.Conn_id) Over (PARTITION By A.tracking_id order by A.first_time ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_first_consignment_conn_id,
first_value(A.eta_in_sec) Over (PARTITION By A.tracking_id order by A.first_time ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_first_consignment_eta_in_sec,
row_number() Over (PARTITION By A.tracking_id order by A.first_time ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as row_n,
first_value(A.consignment_id) Over (PARTITION By A.tracking_id order by A.first_time DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_last_consignment_id,
first_value(A.first_time) Over (PARTITION By A.tracking_id order by A.first_time DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_last_consignment_create_datetime,
first_value(A.Conn_id) Over (PARTITION By A.tracking_id order by A.first_time DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_last_consignment_conn_id,
first_value(A.eta_in_sec) Over (PARTITION By A.tracking_id order by A.first_time DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_last_consignment_eta_in_sec,
row_number() Over (PARTITION By A.tracking_id order by A.first_time DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as l_row_n,
tracking_id
From
(SELECT 
entityid,
cast(split(entityid, "-")[1] AS INT) as consignment_id,
TRID as tracking_id,
`data`.connection_id as Conn_id,
min(`data`.connection_estimated_tat) as eta_in_sec,
min(unix_timestamp(`data`.created_at)) as first_time
FROM 
bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_1_13_view_14d lateral view explode(`data`.shipments) exploded_table as TRID where `data`.type = 'consignment' group by entityid, TRID,`data`.connection_id) A )B
where B.row_n=1 )C )sc
on sv.vendor_tracking_id=sc.vendor_tracking_id
left outer join
(select entityid, 
min(If(`data`.status = 'received' and upper(`data`.current_address.type) IN ('FKL_FACILITY', 'MOTHER_HUB'), from_unixtime(unix_timestamp(`data`.updated_at)),null)) as shipment_inscan_time,
min(If(`data`.shipment_type like '%rto%',from_utc_timestamp(updatedat,'GMT'),null)) as shipment_rto_create_time,
max(If(`data`.status = 'Received',from_unixtime(unix_timestamp(`data`.updated_at)),null)) as fsd_last_received_time,
count(distinct if(`data`.status in ('Out_For_Delivery','out_for_delivery'),concat(`data`.status,to_date(from_unixtime(unix_timestamp(`data`.updated_at)))),NULL)) as fsd_number_of_ofd_attempts,
min(If(`data`.current_address.id = `data`.assigned_address.id and `data`.status = 'Expected',from_unixtime(unix_timestamp(`data`.updated_at)), null)) as fsd_assignedhub_expected_time,
min(If(`data`.current_address.id = `data`.assigned_address.id and `data`.status = 'Received',from_unixtime(unix_timestamp(`data`.updated_at)), null)) as fsd_assignedhub_received_time,
min(If(`data`.status = 'Returned_To_Ekl',from_unixtime(unix_timestamp(`data`.updated_at)),null)) as fsd_returnedtoekl_time,
min(If(`data`.status = 'Received_By_Ekl',from_unixtime(unix_timestamp(`data`.updated_at)),null)) as fsd_receivedbyekl_time,
min(If(lower(`data`.status) IN ('delivered','delivery_update'),from_utc_timestamp(updatedat,'GMT'),NULL)) as runsheet_close_datetime,
min(If(lower(`data`.status) IN ('delivered','delivery_update'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as ekl_delivery_datetime,
min(If(`data`.status in ('Out_For_Delivery','out_for_delivery'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as shipment_first_ofd_datetime ,
max(If(`data`.status in ('Out_For_Delivery','out_for_delivery'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as shipment_last_ofd_datetime,
min(If(`data`.status = 'dispatched_to_vendor',from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as vendor_dispatch_datetime,
min(If(`data`.status = 'undelivered_attempted' AND `data`.vendor_id NOT IN (200,207),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as tpl_first_ofd_time,
max(If(`data`.status = 'undelivered_attempted' AND `data`.vendor_id NOT IN (200,207),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as tpl_last_ofd_time,
min(If(`data`.current_address.id = `data`.assigned_address.id and `data`.status = 'Sent',from_unixtime(unix_timestamp(`data`.updated_at)), null)) as fsd_assigned_hub_sent_datetime,
min(If(lower(`data`.status) = 'undelivered_order_rejected_opendelivery',from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as openbox_reject_datetime,
min(If(`data`.status = 'Expected',from_unixtime(unix_timestamp(`data`.updated_at)),null)) as received_at_source_facility,
min(If(`data`.status IN ('Received','Undelivered_Not_Attended','Error') and `data`.current_address.type in ('DELIVERY_HUB','BULK_HUB'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as fsd_first_dh_received_datetime,
min(If(`data`.status = 'PICKUP_Picked_Complete',from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as rvp_pickup_complete_datetime,
count(distinct if(`data`.status = 'PICKUP_Out_For_Pickup',concat(`data`.status,to_date(from_unixtime(unix_timestamp(`data`.updated_at)))),NULL)) as shipment_rvp_pk_number_of_attempts,
max(If(`data`.status IN ('lost','not_received','reshipped','received_by_merchant','returned_to_seller','delivered'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) as end_state_datetime,
max(If(`data`.status = 'Sent' and `data`.current_address.type in ('DELIVERY_HUB','BULK_HUB'),from_unixtime(unix_timestamp(`data`.updated_at)),
if(`data`.status = 'Expected' and `data`.current_address.type in ('FKL_FACILITY','MOTHER_HUB'),from_unixtime(unix_timestamp(`data`.updated_at)),NULL))) as fsd_last_dhhub_sent_datetime, 
min(`data`.value.value) as shipment_value,
min(`data`.amount_to_collect.value) as COD_amount_to_collect
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4_14d group by entityid
) sreh 
on (sreh.entityid = sv.shipment_id);