insert overwrite table la_runsheet_byod_fact
select
runsheet_info.tasklist_tracking_id as tasklist_tracking_id,
runsheet_info.ekl_facility_id as ekl_facility_id,
runsheet_info.last_tasklist_agent_id as last_tasklist_agent_id,
runsheet_info.last_tasklist_updated_at as last_tasklist_updated_at,
runsheet_info.tasklist_type as tasklist_type,
runsheet_info.vendor_tracking_id as vendor_tracking_id,
runsheet_info.tasklist_status as tasklist_status,
runsheet_info.runsheet_id as runsheet_id,
runsheet_info.agent_id_key as agent_id_key,
runsheet_info.ekl_facility_id_key as ekl_facility_id_key,
runsheet_info.runsheet_created_at as runsheet_created_at,
runsheet_info.runsheet_created_at_date_key as runsheet_created_at_date_key,
ekartappsync.del_vendor_tracking_id as del_vendor_tracking_id,
ekartappsync.shipment_current_status as del_shipment_current_status,
ekartappsync.del_updated_at as del_updated_at,
--ekartappsync.del_fsd_last_current_hub_type as del_fsd_last_current_hub_type,
delivery_update_ekartapp.du_vendor_tracking_id as du_vendor_tracking_id,
delivery_update_ekartapp.du_sign_ship_type as du_sign_ship_type,
delivery_update_ekartapp.du_delivery_status as du_delivery_status,
delivery_update_ekartapp.du_ship_sign_type as du_ship_sign_type,
delivery_update_ekartapp.du_source as du_source,
delivery_update_ekartapp.du_create_date as du_create_date,
delivery_update_ekartapp.du_updated_date as du_updated_date,
delivery_update_ekartapp.du_devicelatitude as du_devicelatitude,
delivery_update_ekartapp.du_devicelongitude as du_devicelongitude,
delivery_update_ekartapp.du_batterylevel as du_batterylevel,
delivery_update_ekartapp.du_status as du_status,
ekartappdevice.agent_id as agent_id,
ekartappdevice.employee_id as employee_id,
ekartappdevice.hubid as hubid,
ekartappdevice.imei1 as imei1,
ekartappdevice.imei2 as imei2,
ekartappdevice.created_date as created_date,
ekartappdevice.updated_date as updated_date,
ekartappdevice.source as source,
ekartappdevice.devicemodel as devicemodel,
ekartappdevice.appversion as appversion,
ekartappdevice.ossdk as ossdk,
ekartappdevice.osversion as osversion
from
(select
tasklist_tracking_id as tasklist_tracking_id,
ekl_facility_id as ekl_facility_id,
last_tasklist_agent_id as last_tasklist_agent_id,
last_tasklist_updated_at as last_tasklist_updated_at,
tasklist_type as tasklist_type,
last_tasklist_id as vendor_tracking_id,
tasklist_status as tasklist_status,
runsheet_id as runsheet_id,
last_tasklist_agent_id_key as agent_id_key,
ekl_facility_id_key as ekl_facility_id_key,
runsheet_created_at as runsheet_created_at,
runsheet_created_at_date_key as runsheet_created_at_date_key
from bigfoot_external_neo.scp_ekl__la_shipment_runsheet_fact)runsheet_info
left outer join
(select T.del_vendor_tracking_id as del_vendor_tracking_id,T.del_updated_at as del_updated_at,T.del_fsd_last_current_hub_type_l as del_fsd_last_current_hub_type,T.shipment_current_status,T.row_n
from
(SELECT `data`.vendor_tracking_id as del_vendor_tracking_id ,`data`.updated_at as del_updated_at,`data`.current_address.type as del_fsd_last_current_hub_type_l,lower(`data`.status) as shipment_current_status,
row_number() Over ( PARTITION BY `data`.vendor_tracking_id ORDER BY `data`.updated_at desc ) as row_n
FROM  bigfoot_journal.dart_wsr_scp_ekl_shipment_4 where lower(`data`.status) in ('delivered','delivery_update'))T where T.row_n=1)ekartappsync
on runsheet_info.vendor_tracking_id=ekartappsync.del_vendor_tracking_id
left outer join
(select
appsync_2.vendor_tracking_id as du_vendor_tracking_id,
appsync_2.sign_ship_type as du_sign_ship_type,
appsync_2.delivery_status as du_delivery_status,
appsync_2.ship_sign_type as du_ship_sign_type,
appsync_2.source as du_source,
appsync_2.create_date as du_create_date,
appsync_2.updated_date as du_updated_date,
appsync_2.devicelatitude as du_devicelatitude,
appsync_2.devicelongitude as du_devicelongitude,
appsync_2.batterylevel as du_batterylevel,
appsync_2.status as du_status,
appsync_2.row_n as row_n
from
(select
appsync_du.vendor_tracking_id as vendor_tracking_id,
appsync_du.sign_ship_type as sign_ship_type,
appsync_du.delivery_status as delivery_status,
appsync_du.ship_sign_type as ship_sign_type,
appsync_du.source as source,
appsync_du.create_date as create_date,
appsync_du.updated_date as updated_date,
appsync_du.devicelatitude as devicelatitude,
appsync_du.devicelongitude as devicelongitude,
appsync_du.batterylevel as batterylevel,
FIRST_VALUE(appsync_du.delivery_status) OVER (PARTITION BY appsync_du.vendor_tracking_id ORDER BY appsync_du.create_date asc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS status,
row_number() Over ( PARTITION BY appsync_du.vendor_tracking_id ORDER BY appsync_du.create_date asc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as row_n
from
(Select
`data`.id as vendor_tracking_id,
`data`.entitytype as sign_ship_type,
`data`.eventtype as delivery_status,	
`data`.type as ship_sign_type,
`data`.source as source,
`data`.createdat as create_date,
`data`.uploadedat as updated_date,
`data`.devicelatitude as devicelatitude,
`data`.devicelongitude as devicelongitude,
`data`.batterylevel as batterylevel
from
bigfoot_journal.dart_wsr_scp_ekl_ekartappsync_1
where 
lower(`data`.eventtype) in ('delivery_update') 
and lower(`data`.entitytype) in ('forward_shipment') 
and lower(`data`.type) in ('shipment_status')
) appsync_du
) appsync_2
where appsync_2.row_n=1)delivery_update_ekartapp
on runsheet_info.vendor_tracking_id=delivery_update_ekartapp.DU_vendor_tracking_id
left outer join
(select
appdevice.agent_id as agent_id,
appdevice.fhrid as employee_id,
appdevice.hubid as hubid,
appdevice.imei1 as imei1,
appdevice.imei2 as imei2,
appdevice.createdat as created_date,
appdevice.updatedat as updated_date,
appdevice.source as source,
appdevice.devicemodel as devicemodel,
appdevice.appversion as appversion,
appdevice.ossdk as ossdk,
appdevice.osversion as osversion,
appdevice.row_n as row_n
from
(Select
`data`.agentid as agent_id,
`data`.fhrId as fhrid,
`data`.hubId as hubid,
`data`.imei1 as imei1,
`data`.imei2 as imei2,
`data`.createdAt as createdat,
`data`.updatedAt as updatedat,
`data`.source as source,
`data`.deviceModel as devicemodel,
`data`.appVersion as appversion,
`data`.osSdk as ossdk,
`data`.osVersion as osversion,
row_number() Over ( PARTITION BY `data`.agentid ORDER BY `data`.createdAt desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING ) as row_n
from bigfoot_journal.dart_wsr_scp_ekl_ekartappdevice_1_0)appdevice where appdevice.row_n=1)ekartappdevice
on runsheet_info.last_tasklist_agent_id=ekartappdevice.agent_id;