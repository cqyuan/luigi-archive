CREATE TABLE pebble.ztest_developers AS


select identity_0_user, min(time) as time
from pebble.phone_events
where event = 'sdk_app_installed' 
and time >= 1391385600
and time <= to_unixtime(now())
group by identity_0_user