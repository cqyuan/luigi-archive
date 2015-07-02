CREATE TABLE pebble.ztest_product_active_hb_maxtimes AS
select serial_number, max(time) maxtime_active_watch
from system_metrics_clean
where time >= 1402444800
and time <= to_unixtime(now())
and user_id not in ('')
and serial_number not in ('', 'XXXXXXXXXXXX', '$$pebble_id$$')
and length(serial_number) = 12
and hours_on_wrist > 0
group by serial_number