insert overwrite table la_shipment_runsheet_fact
select
C_runsheet.tasklist_tracking_id as tasklist_tracking_id,
C_runsheet.ekl_facility_id as ekl_facility_id,
C_runsheet.agent_id as last_tasklist_agent_id,
C_runsheet.updated_at as last_tasklist_updated_at,
C_runsheet.document_type as tasklist_type,
C_runsheet.tasklist_id as last_tasklist_id,
C_runsheet.status as tasklist_status,
C_runsheet.runsheet_id as runsheet_id,
lookupkey('agent_id',C_runsheet.agent_id) as last_tasklist_agent_id_key,
lookupkey('facility_id',C_runsheet.ekl_facility_id) as ekl_facility_id_key,
C_runsheet.created_at as runsheet_created_at,
lookup_date(to_date(C_runsheet.created_at)) as runsheet_created_at_date_key,
'dummy1' as dummy1,
'dummy2' as dummy2
from
(select
B_runsheet.task_id as tasklist_tracking_id,
B_runsheet.ekl_facility_id as ekl_facility_id,
B_runsheet.agent_id as agent_id,
B_runsheet.created_at as created_at,
B_runsheet.updated_at as updated_at,
B_runsheet.document_type as document_type,
B_runsheet.tasklist_id as tasklist_id,
B_runsheet.status as status,
B_runsheet.runsheet_ids as runsheet_id_n,
FIRST_VALUE(B_runsheet.runsheet_ids) OVER (PARTITION BY B_runsheet.tasklist_id ORDER BY B_runsheet.updated_at desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS runsheet_id,
row_number() Over (PARTITION BY B_runsheet.tasklist_id ORDER BY B_runsheet.updated_at desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as row_n
from
(select
A_Runsheet.agent_id as agent_id,
A_Runsheet.created_at as created_at,
A_Runsheet.ekl_facility_id as ekl_facility_id,
A_Runsheet.updated_at as updated_at,
A_Runsheet.document_type as document_type,
A_Runsheet.task_id as task_id,
A_Runsheet.status as status,
regexp_extract(A_Runsheet.task_id, '^[0-9]*(-)(.*)', 2) as tasklist_id,
regexp_extract(entityid,'^([a-zA-Z]*)(-)(.*)', 3) as runsheet_ids
from
(select entityid ,`data`.agent_id as agent_id,`data`.created_at as created_at,`data`.document_type as document_type,`data`.status as status,`data`.ekl_facility_id as ekl_facility_id,`data`.updated_at as updated_at,task_id from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view LATERAL VIEW EXPLODE(`data`.task_ids)task_id as task_id where `data`.document_type in ('runsheet','pickupsheet')
and `data`.ekl_facility_id in (23,62,141,466,470,471,497,498,499,500,501,589,590,591,592,593,594,595,622,646,653,709,1199,1409,1684,1804))A_Runsheet )B_runsheet )C_runsheet where C_runsheet.row_n=1;

