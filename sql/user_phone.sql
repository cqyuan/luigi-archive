CREATE TABLE ztest_user_phone AS
select identity_0_user user_id, identity_0_device phone_id, platform, min(time) mintime_phone, max(time) maxtime_phone
from phone_events
where time >= 1391385600
and time <= to_unixtime(now())
and identity_0_user not in ('')
and identity_0_device not in ('', '$$phone_id$$pid=$$pebble_id$$', '$$phone_id$$')
and (length(identity_0_device) = 16 or length(identity_0_device) = 36)
and platform in ('ios', 'android')
and identity_0_user like '5%'
group by identity_0_user, identity_0_device, platform