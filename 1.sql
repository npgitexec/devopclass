EXPLAIN WITH service_fleet_measures_times_period AS (SELECT 
	sms_users.id  AS user_id,
	ext_mt_aircraft.id  AS aircraft_id,
	flights_time_periods.id  AS flight_id,
	flights.actual_arrival_tstamp >= (SELECT (((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL))))
      AND flights.actual_arrival_tstamp < (SELECT ((((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL)))) AS in_current_period,
	flights.actual_arrival_tstamp >=  (SELECT (((((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL)) - (((SELECT (EXTRACT(EPOCH FROM (((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL))) - EXTRACT(EPOCH FROM ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL))))))||'seconds')::INTERVAL)::TIMESTAMPTZ)))
      AND flights.actual_arrival_tstamp < (SELECT (((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL))))  AS in_previous_period,
	COALESCE(SUM(((ifc_availability_result.result->'raw'->>'success')::NUMERIC) ), 0) AS success,
	COALESCE(SUM(((ifc_availability_result.result->'raw'->>'total')::NUMERIC) ), 0) AS total
FROM sms.flights  AS flights_time_periods
INNER JOIN sms.flights  AS flights ON flights_time_periods.id = flights.id 
INNER JOIN sms.mt_aircraft  AS ext_mt_aircraft ON flights.aircraft_id = ext_mt_aircraft.id 
INNER JOIN sms.sms_users  AS sms_users ON ext_mt_aircraft.user_id = sms_users.id 
LEFT JOIN sla.sla_configuration  AS availability_source_conf_per_flight ON sms_users.id = availability_source_conf_per_flight.airline_id
            AND flights.actual_departure_tstamp BETWEEN availability_source_conf_per_flight.activated_tstamp
            AND coalesce(availability_source_conf_per_flight.deactivated_tstamp, timestamp_immutable()) 
LEFT JOIN sla.sla_configuration  AS ifc_conf_per_flight ON sms_users.id = ifc_conf_per_flight.airline_id
            AND flights.actual_departure_tstamp BETWEEN availability_source_conf_per_flight.activated_tstamp
            AND coalesce(availability_source_conf_per_flight.deactivated_tstamp, timestamp_immutable()) 
LEFT JOIN sla.sla_scorecard  AS ifc_availability_result ON flights.id = ifc_availability_result.flight_id AND ifc_conf_per_flight.id = ifc_availability_result.config_id 

WHERE (availability_source_conf_per_flight.category = 'availability_defn'::sla.SLA_CATEGORY
      AND availability_source_conf_per_flight.rule_type = 'config') AND (ifc_conf_per_flight.category = (availability_source_conf_per_flight.rule_params->>'category_to_use')::sla.SLA_CATEGORY
              AND ifc_conf_per_flight.rule_type = 'measure'
              AND (ifc_conf_per_flight.rule_params ->> 'kpi') IN ('packetsLost', 'test_result')) AND (flights.actual_arrival_tstamp >=  (SELECT (((((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL)) - (((SELECT (EXTRACT(EPOCH FROM (((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL))) - EXTRACT(EPOCH FROM ((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL))))))||'seconds')::INTERVAL)::TIMESTAMPTZ)))
      AND flights.actual_arrival_tstamp < (SELECT ((((DATE_TRUNC('day', CURRENT_TIMESTAMP) + (-1 || ' day')::INTERVAL) + (1 || ' day')::INTERVAL))))) AND (sms_users.name = 'American Airlines') AND (sms_users.name LIKE '%')
GROUP BY 1,2,3,4,5)
SELECT 
	((((COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))-(COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END)))*100)/ NULLIF((COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END)),0)) AS "service_fleet_measures_times_period.flight_count_change_pct",
	(COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))-(COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END)) AS "service_fleet_measures_times_period.ifc_impaired_flight_count",
	(((((COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))-(COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END)))-((COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))-(COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))))*100)/ NULLIF(((COUNT(DISTINCT CASE WHEN (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))-(COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))),0)) AS "sfmtp.ifc_impaired_flight_count_change_pct",
	COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END) AS "service_fleet_measures_times_period.online_flight_count",
	((((COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_current_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END))-(COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END)))*100)/ NULLIF((COUNT(DISTINCT CASE WHEN (COALESCE(service_fleet_measures_times_period.success, 0) > 0) AND (service_fleet_measures_times_period.in_previous_period = 'yes') THEN service_fleet_measures_times_period.flight_id  ELSE NULL END)),0)) AS "sfmtp.online_flight_count_change_pct"
FROM service_fleet_measures_times_period
