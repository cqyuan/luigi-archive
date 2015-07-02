CREATE TABLE ztest_product_table AS
select serial_number, product_type, product_version, sales_channel, 
last_user_id, first_user_id, number_users,
last_phone_id, first_phone_id,number_phones,
last_platform, first_platform, multi_platform, 
first_seen, last_seen_phone, last_seen_watch, last_seen_active_watch

from (select ztest_product_list.serial_number serial_number, 
ztest_product_list.product_type product_type, 
ztest_product_list.product_version product_version, 
ztest_product_list.sales_channel sales_channel, 
last_user.user_id last_user_id, 
first_user.user_id first_user_id, 
users.user_count number_users,
last_phone.phone_id last_phone_id, 
first_phone.phone_id first_phone_id,
phones.phone_count number_phones,
last_phone.platform last_platform, 
first_phone.platform first_platform, 
if(phones.platform_count = 1, 'false', if(phones.platform_count >1, 'true', NULL)) multi_platform, 
ztest_product_list.mintime_phone first_seen, 
ztest_product_list.maxtime_phone last_seen_phone, 
ztest_product_hb_times.maxtime_watch last_seen_watch, 
ztest_product_hb_times.maxtime_active_watch last_seen_active_watch, count(1)

from ztest_product_list

left join (select ztest_user_product.serial_number serial_number, user_id
from ztest_user_product
join (select serial_number, min(mintime_phone) mintime_phone
from ztest_user_product
where user_id in (select id from pebble_personal_info.user_list)
group by serial_number) mintimes
on ztest_user_product.serial_number = mintimes.serial_number
where ztest_user_product.mintime_phone = mintimes.mintime_phone
and user_id in (select id from pebble_personal_info.user_list)) first_user
on ztest_product_list.serial_number = first_user.serial_number

left join (select ztest_user_product.serial_number serial_number, user_id
from ztest_user_product
join (select serial_number, max(maxtime_phone) maxtime_phone
from ztest_user_product
where user_id in (select id from pebble_personal_info.user_list)
group by serial_number) maxtimes
on ztest_user_product.serial_number = maxtimes.serial_number
where ztest_user_product.maxtime_phone = maxtimes.maxtime_phone
and user_id in (select id from pebble_personal_info.user_list)) last_user
on ztest_product_list.serial_number = last_user.serial_number

left join (select serial_number, count(distinct user_id) user_count
from ztest_user_product
where user_id in (select id from pebble_personal_info.user_list)
group by serial_number) users
on ztest_product_list.serial_number = users.serial_number

left join (select ztest_product_phone.serial_number serial_number, upper(phone_id) phone_id, platform
from ztest_product_phone
join (select serial_number, min(mintime_phone) mintime_phone
from ztest_product_phone
group by serial_number) mintimes
on ztest_product_phone.serial_number = mintimes.serial_number
where ztest_product_phone.mintime_phone = mintimes.mintime_phone) first_phone
on ztest_product_list.serial_number = first_phone.serial_number

left join (select ztest_product_phone.serial_number serial_number, upper(phone_id) phone_id, platform
from ztest_product_phone
join (select serial_number, max(maxtime_phone) maxtime_phone
from ztest_product_phone
group by serial_number) maxtimes
on ztest_product_phone.serial_number = maxtimes.serial_number
where ztest_product_phone.maxtime_phone = maxtimes.maxtime_phone) last_phone
on ztest_product_list.serial_number = last_phone.serial_number

left join (select serial_number, count(distinct upper(phone_id)) phone_count, count(distinct platform) platform_count
from ztest_product_phone
group by serial_number) phones
on ztest_product_list.serial_number = phones.serial_number

left join ztest_product_hb_times
on ztest_product_list.serial_number = ztest_product_hb_times.serial_number

group by ztest_product_list.serial_number, 
ztest_product_list.product_type, 
ztest_product_list.product_version, 
ztest_product_list.sales_channel, 
last_user.user_id, 
first_user.user_id, 
users.user_count,
last_phone.phone_id, 
first_phone.phone_id,
phones.phone_count,
last_phone.platform, 
first_phone.platform, 
if(phones.platform_count = 1, 'false', if(phones.platform_count >1, 'true', NULL)), 
ztest_product_list.mintime_phone, 
ztest_product_list.maxtime_phone, 
ztest_product_hb_times.maxtime_watch, 
ztest_product_hb_times.maxtime_active_watch)