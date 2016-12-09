INSERT overwrite TABLE la_shipment_l0_snapshot_fact
SELECT entityid AS entityid,
	`data`.vendor_tracking_id AS vendor_tracking_id,
	`data`.STATUS AS shipment_current_status,
	updatedat AS updatedat,
	`data`.payment_type AS payment_type,
	`data`.shipment_type AS shipment_type,
	`data`.customer_sla AS customer_sla,
	`data`.design_sla AS design_sla,
	`data`.created_at AS created_at,
	`data`.vendor_id AS vendor_id,
	`data`.assigned_address.id AS fsd_assigned_hub_id,
	`data`.current_address.id AS fsd_last_current_hub_id,
	`data`.current_address.type AS fsd_last_current_hub_type,
	`data`.billable_weight AS ekl_billable_weight,
	`data`.source_address.id AS source_address_id,
	`data`.source_address.pincode AS rvp_origin_geo_id,
	`data`.destination_address.id AS destination_address_id,
	`data`.shipment_items [0].seller_id AS seller_id, 
	`data`.payment.payment_details [0].device_id AS pos_id, 
	`data`.payment.payment_details [0].transaction_id AS transaction_id, 
	`data`.payment.payment_details [0].agent_id AS agent_id,
	`data`.payment.amount_collected.value AS amount_collected,
	`data`.destination_address.type AS destination_address_type,
	`data`.source_address.type AS seller_type,
	concat_ws("-",`data`.attributes) AS sv_attribute,
	IF (`data`.shipment_type = 'rvp',`data`.source_address.pincode,`data`.destination_address.pincode) AS location_pincode,
	IF (`data`.shipment_type = 'rvp',`data`.destination_address.pincode,`data`.source_address.pincode) AS source_address_pincode,
	IF (`data`.shipment_type = 'rvp',`data`.source_address.pincode,`data`.destination_address.pincode) AS destination_address_pincode
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view_total
	WHERE (`data`.source_address.id IN (563,564 ,565 ,566 ,567 ,1280 ,1282 ,1288 ,1511 ,1757 ,1950 ,2043 ,3594 ,3612 ,3620 ,3621 ,3622 ,3623)
		OR `data`.destination_address.id IN (563 ,564 ,565 ,566 ,567 ,1280 ,1282 ,1288 ,1511, 1757 ,1950 ,2043 ,3594 ,3612 ,3620 ,3621 ,3622 ,3623)
		AND (`data`.vendor_id IN (200 ,207 ,242) OR `data`.vendor_id = '')
		AND (updatedat > ((unix_timestamp() - (86400 * 180)) * 1000)));