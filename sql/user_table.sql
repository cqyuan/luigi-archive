CREATE TABLE pebble_personal_info.ztest_user_table AS
select user_id, email, name, roles, gender, developer,
country, region, city, metro_code,
current_sign_in_ip, last_sign_in_ip, state, sign_in_count,
last_serial_number, first_serial_number, number_products, upgrader,
last_phone_id, first_phone_id, number_phones,
last_platform, first_platform, multi_platform, 
account_creation_time, first_seen, last_seen_phone, last_seen_watch, last_seen_active_watch,
if((last_platform = 'ios' and multi_platform = 'false'
and cast(last_seen_watch as double) < to_unixtime(now()) - (60*60*24*14)
and floor((cast(last_seen_watch as double) - cast(last_seen_phone as double))/(60*60*24)) < -1), 'yes', 'no') bad_chronology

from (select pebble_personal_info.user_list.id user_id, pebble_personal_info.user_list.email email, pebble_personal_info.user_list.name name, pebble_personal_info.user_list.roles roles, 
if(gender = 'male', 'male', if(gender = 'female', 'female', 'unknown')) gender,
if(pebble_personal_info.user_list.id in (select identity_0_user from ztest_developers), 'yes', 'no') developer,
case when location.current_country is not null then location.current_country else location.last_country
end country,
case when location.current_region is not null then location.current_region else location.last_region
end region,
case when location.current_city is not null then location.current_city else location.last_city
end city,
case when location.current_metro_code is not null then location.current_metro_code else location.last_metro_code
end metro_code,
pebble_personal_info.user_list.current_sign_in_ip current_sign_in_ip, pebble_personal_info.user_list.last_sign_in_ip last_sign_in_ip, 
pebble_personal_info.user_list.state state, pebble_personal_info.user_list.sign_in_count sign_in_count,
last_product.serial_number last_serial_number, 
first_product.serial_number first_serial_number, 
products.product_count number_products,
if(pebble_personal_info.user_list.id in (select ztest_user_product.user_id user_id
from ztest_user_product
join (select user_id, min(mintime_phone) mintime
from ztest_user_product
where serial_number like 'Q2%'
group by user_id) steels
on ztest_user_product.user_id = steels.user_id
where ztest_user_product.serial_number not like 'Q2%'), 'yes', 'no') upgrader,
last_phone.phone_id last_phone_id, 
first_phone.phone_id first_phone_id, 
phones.phone_count number_phones,
last_phone.platform last_platform, 
first_phone.platform first_platform, 
if(phones.platform_count = 1, 'false', if(phones.platform_count > 1, 'true', null)) multi_platform, 
pebble_personal_info.user_list.time account_creation_time, 
phonetimes.mintime_phone first_seen,
phonetimes.maxtime_phone last_seen_phone, 
ztest_user_hb_times.maxtime_watch last_seen_watch, 
ztest_user_hb_times.maxtime_active_watch last_seen_active_watch, count(1)
from pebble_personal_info.user_list

left join location
on pebble_personal_info.user_list.id = location.user_id

left join gender
on pebble_personal_info.user_list.id = gender.user_id

left join (select ztest_user_product.user_id user_id, serial_number
from ztest_user_product
join (select user_id, max(maxtime_phone) maxtime_phone
from ztest_user_product
group by user_id) maxtimes
on ztest_user_product.user_id = maxtimes.user_id
where ztest_user_product.maxtime_phone = maxtimes.maxtime_phone) last_product
on pebble_personal_info.user_list.id = last_product.user_id

left join (select ztest_user_product.user_id user_id, serial_number
from ztest_user_product
join (select user_id, min(mintime_phone) mintime_phone
from ztest_user_product
group by user_id) mintimes
on ztest_user_product.user_id = mintimes.user_id
where ztest_user_product.mintime_phone = mintimes.mintime_phone) first_product
on pebble_personal_info.user_list.id = first_product.user_id

left join (select user_id, count(distinct serial_number) product_count
from ztest_user_product
group by user_id) products
on pebble_personal_info.user_list.id = products.user_id

left join (select ztest_user_phone.user_id user_id, upper(phone_id) phone_id, platform
from ztest_user_phone
join (select user_id, max(maxtime_phone) maxtime_phone
from ztest_user_phone
group by user_id) maxtimes
on ztest_user_phone.user_id = maxtimes.user_id
where ztest_user_phone.maxtime_phone = maxtimes.maxtime_phone) last_phone
on pebble_personal_info.user_list.id = last_phone.user_id

left join (select ztest_user_phone.user_id user_id, upper(phone_id) phone_id, platform
from ztest_user_phone
join (select user_id, min(mintime_phone) mintime_phone
from ztest_user_phone
group by user_id) mintimes
on ztest_user_phone.user_id = mintimes.user_id
where ztest_user_phone.mintime_phone = mintimes.mintime_phone) first_phone
on pebble_personal_info.user_list.id = first_phone.user_id

left join (select user_id, count(distinct upper(phone_id)) phone_count, count(distinct platform) platform_count
from ztest_user_phone
group by user_id) phones
on pebble_personal_info.user_list.id = phones.user_id

left join (select user_id, min(mintime_phone) mintime_phone, max(maxtime_phone) maxtime_phone
from ztest_user_product
group by user_id) phonetimes
on pebble_personal_info.user_list.id = phonetimes.user_id

left join ztest_user_hb_times
on pebble_personal_info.user_list.id = ztest_user_hb_times.user_id

group by pebble_personal_info.user_list.id, pebble_personal_info.user_list.email, pebble_personal_info.user_list.name, pebble_personal_info.user_list.roles, 
if(gender = 'male', 'male', if(gender = 'female', 'female', 'unknown')),
if(pebble_personal_info.user_list.id in (select identity_0_user from ztest_developers), 'yes', 'no'),
case when location.current_country is not null then location.current_country else location.last_country
end,
case when location.current_region is not null then location.current_region else location.last_region
end,
case when location.current_city is not null then location.current_city else location.last_city
end,
case when location.current_metro_code is not null then location.current_metro_code else location.last_metro_code
end,
pebble_personal_info.user_list.current_sign_in_ip, pebble_personal_info.user_list.last_sign_in_ip, pebble_personal_info.user_list.state, pebble_personal_info.user_list.sign_in_count, 
last_product.serial_number, 
first_product.serial_number, 
products.product_count,
if(pebble_personal_info.user_list.id in (select ztest_user_product.user_id user_id
from ztest_user_product
join (select user_id, min(mintime_phone) mintime
from ztest_user_product
where serial_number like 'Q2%'
group by user_id) steels
on ztest_user_product.user_id = steels.user_id
where ztest_user_product.serial_number not like 'Q2%'), 'yes', 'no'),
last_phone.phone_id, 
first_phone.phone_id, 
phones.phone_count,
last_phone.platform, 
first_phone.platform, 
if(phones.platform_count = 1, 'false', if(phones.platform_count > 1, 'true', null)), 
pebble_personal_info.user_list.time, 
phonetimes.mintime_phone,
phonetimes.maxtime_phone, 
ztest_user_hb_times.maxtime_watch, 
ztest_user_hb_times.maxtime_active_watch)