insert overwrite table la_14d_slot_fact
SELECT distinct
sl.slot_id as slot_id,
sl.slot_code as slot_code,
sl.slot_facility_id as slot_facility_id,
sl.slot_start_date as slot_start_date,
sl.slot_end_date as slot_end_date,
sl.slot_actual_capacity as slot_actual_capacity,
sl.slot_shipment_size as slot_shipment_size,
sl.slot_duration as slot_duration,
sr.slot_booking_id as slot_booking_id,
sr.slot_reservation_status as slot_reservation_status,
sr.vendor_tracking_id as vendor_tracking_id
from
(select
`data`.slot_id as slot_id,
`data`.slot_code as slot_code,
`data`.facility_id as slot_facility_id,
`data`.start_date as slot_start_date,
`data`.end_date as slot_end_date,
`data`.capability_matrix.actual_capacity as slot_actual_capacity,
`data`.capability_matrix.shipment_size as slot_shipment_size,
`data`.duration as slot_duration
from bigfoot_snapshot.dart_wsr_scp_ekl_facilityslot_1_view_total
where `data`.slot_type in ('large','LARGE','VERYLARGE') and `data`.slot_current_state='enabled') sl
left outer join 
(select
`data`.reservation_id as slot_booking_id,
`data`.reservation_asset_id,
`data`.slot_reservation_status as slot_reservation_status,
`data`.reservation_entity_id as vendor_tracking_id
from bigfoot_snapshot.dart_wsr_scp_ekl_slotreservation_1_view_total 
where `data`.slot_reservation_status not in ('CANCELLED')) sr
on sl.slot_id=sr.reservation_asset_id;