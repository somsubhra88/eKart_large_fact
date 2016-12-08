Insert overwrite table la_snop_dc_cutoff_outbound_fact
select plan.zone_name,
actual.DC as dc_name,
plan.LM_HUB as hub,
actual.dc_cutoff_time as dc_cutoff,
plan.dc_month,
actual.act_dc_date as dc_date,
plan.plan_value,
actual_value,
lookup_date(actual.act_dc_date) as dc_date_key,
actual.vendor_name as vendor_name 
from 
(select 
to_date(fulfill_item_unit_dispatch_actual_time) as act_dc_date,
upper(trim(order_item_unit_source_facility)) as DC, 
cast(dc_cutoff_time as int) as dc_cutoff_time,
vendor_name,
sum(combined_order_item_unit_quantity) as actual_value
from  
(select 
snop.fulfill_item_unit_dispatch_actual_time,
snop.order_item_unit_source_facility,
snop.dc_cutoff_time,
snop.combined_order_item_unit_quantity,
vendor_dim.vendor_name as vendor_name 
from bigfoot_external_neo.scp_ekl__la_snop_hive_fact snop
 left outer join 
( Select logistics_vendor_hive_dim_key, 
(case when vendor_name like '%safex%' then 'SAFEX' else upper(trim(vendor_name)) end) as vendor_name from 
 bigfoot_external_neo.scp_ekl__logistics_vendor_hive_dim ) vendor_dim
on snop.vendor_id_key = vendor_dim.logistics_vendor_hive_dim_key
) actual
group by 
upper(trim(order_item_unit_source_facility)),
dc_cutoff_time,
to_date(fulfill_item_unit_dispatch_actual_time),
vendor_name) actual
left outer join
(select upper(zone_name) as zone_name,
dc_date as dc_date,
upper(trim(dc_name)) as dc_name,
Upper(trim(lm_hub)) as lm_hub,
cast((case when DC_cutoff = 930 then '2130' else DC_cutoff end ) as int) as DC_cutoff,
upper(trim(vendor)) as vendor,
upper(trim(dc_month)) as dc_month,
plan_value
from bigfoot_common.la_snop_dc_cutoff_outbound_v1) plan
on ( actual.DC = plan.dc_name and
actual.act_dc_date = plan.dc_date and 
actual.dc_cutoff_time  = plan.DC_cutoff and
actual.vendor_name = plan.vendor);