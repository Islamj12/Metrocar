/* 
Query 1:
This code is for the Funnel analysis for the users and rides across every step in the metrocar company which consists of 7 steps.
*/

WITH num_users_rides AS (
    SELECT
        ad.platform, 
        COALESCE(s.age_range, 'Unknown') AS age_ranges,
        DATE(ad.download_ts) AS download_dt,
  
-- Counting Number Of Users:
  			COUNT(DISTINCT ad.app_download_key) AS num_downloads,
        COUNT(DISTINCT s.user_id) AS num_users_signed_up,
        COUNT(DISTINCT rr.user_id) AS num_users_requested_rides,
        COUNT(DISTINCT CASE WHEN rr.accept_ts IS NOT NULL THEN rr.user_id END) AS num_users_accepted_rides,
        COUNT(DISTINCT CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.user_id END) AS num_users_completed_rides,
        COUNT(DISTINCT CASE WHEN tr.charge_status = 'Approved' THEN rr.user_id END) AS num_users_paid,
        COUNT(DISTINCT rv.user_id) AS num_users_with_review, 
  
-- Counting Number Of Rides: 
        COUNT(rr.ride_id) AS num_requested_rides,
  			COUNT(DISTINCT CASE WHEN rr.accept_ts IS NOT NULL THEN rr.ride_id END) AS num_accepted_rides,
  			COUNT(DISTINCT CASE WHEN rr.dropoff_ts IS NOT NULL THEN rr.ride_id END) AS num_completed_rides,
				COUNT(DISTINCT CASE WHEN tr.charge_status = 'Approved' THEN rr.ride_id END) AS num_profitable_rides, 			
        COUNT(DISTINCT rv.ride_id)	AS num_reviewed_rides
    
  FROM app_downloads ad
    LEFT JOIN signups s ON ad.app_download_key = s.session_id
    LEFT JOIN ride_requests rr ON s.user_id = rr.user_id
    LEFT JOIN transactions tr ON tr.ride_id = rr.ride_id
    LEFT JOIN reviews rv ON rv.ride_id = rr.ride_id
    GROUP BY ad.platform, age_ranges, download_dt 
),

-- creating the Funnel steps:
steps AS (
    SELECT
        0 AS funnel_step,
        'downloads' AS funnel_name,
        num_downloads AS num_users, 
  			0 AS num_rides,
  			platform, age_ranges, download_dt
    FROM num_users_rides

    UNION ALL

    SELECT
        1 AS funnel_step,
        'signups' AS funnel_name,
        num_users_signed_up AS num_users, 
  			0 AS num_rides,
  			platform, age_ranges, download_dt
    FROM num_users_rides

    UNION ALL

    SELECT
        2 AS funnel_step,
        'ride_requested' AS funnel_name,
        num_users_requested_rides AS num_users, 
  			num_requested_rides AS num_rides,
  			platform, age_ranges, download_dt
    FROM num_users_rides

    UNION ALL

    SELECT
        3 AS funnel_step,
        'ride_accepted' AS funnel_name,
        num_users_accepted_rides AS num_users, 
  			num_accepted_rides AS num_rides,
  			platform , age_ranges, download_dt 
    FROM num_users_rides

    UNION ALL

    SELECT
        4 AS funnel_step,
        'ride_completed' AS funnel_name,
        num_users_completed_rides AS num_users, 
  			num_completed_rides AS num_rides,
  			platform , age_ranges, download_dt
    FROM num_users_rides

    UNION ALL

    SELECT
        5 AS funnel_step,
        'payment' AS funnel_name,
        num_users_paid AS num_users,
  			num_profitable_rides AS num_rides,
  			platform , age_ranges, download_dt 
    FROM num_users_rides

    UNION ALL

    SELECT
        6 AS funnel_step,
        'Reviews' AS funnel_name,
        num_users_with_review AS num_users,
  			num_reviewed_rides AS num_rides,
  			platform, age_ranges, download_dt
    FROM num_users_rides
)

SELECT 
	funnel_step, 
	funnel_name, 
  platform, 
  age_ranges,
  TO_CHAR(download_dt, 'FMMM/FMDD/YYYY') AS download_dt_formatted,
  num_users, num_rides
FROM steps 
ORDER BY funnel_step, funnel_name, platform, age_ranges, download_dt
;

/* 
Query 2:
This Code is To see the Distribution of ride requests and users throughout the day(per Hour):
*/

SELECT EXTRACT(HOUR FROM rr.request_ts) AS hour_of_day,
COUNT(rr.ride_id) AS num_rides_requested,
COUNT(DISTINCT rr.user_id) AS num_users
FROM ride_requests rr
INNER JOIN signups s ON rr.user_id = s.user_id
GROUP BY hour_of_day
ORDER BY hour_of_day
;
