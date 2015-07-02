CREATE TABLE ztest_user_product AS
select identity_0_user user_id, identity_0_serial_number serial_number, min(time) mintime_phone, max(time) maxtime_phone
from phone_events
where time >= 1391385600
and time <= to_unixtime(now())
and identity_0_user not in ('')
and identity_0_serial_number not in ('', 'XXXXXXXXXXXX', '$$pebble_id$$')
and length(identity_0_serial_number) = 12
and identity_0_user like '5%'
group by identity_0_user, identity_0_serial_number