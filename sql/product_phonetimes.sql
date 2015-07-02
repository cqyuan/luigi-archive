CREATE TABLE ztest_product_phonetimes AS
select identity_0_serial_number serial_number, min(time) mintime_phone, max(time) maxtime_phone
from phone_events
where time >= 1391385600
and time <= to_unixtime(now())
and identity_0_serial_number not in ('', 'XXXXXXXXXXXX', '$$pebble_id$$')
and length(identity_0_serial_number) = 12
group by identity_0_serial_number