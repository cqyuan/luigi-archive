CREATE TABLE pebble.ztest_user_app_hb_maxtimes AS
select identity_0_user user_id, max(time) maxtime_watch
from remote_device_app_metrics
where time >= 1402444800
and time <= to_unixtime(now())
and identity_0_user not in ('')
and identity_0_serial_number not in ('', 'XXXXXXXXXXXX', '$$pebble_id$$')
and length(identity_0_serial_number) = 12
and identity_0_user like '5%'
group by identity_0_user