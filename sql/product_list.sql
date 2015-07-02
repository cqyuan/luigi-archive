CREATE TABLE ztest_product_list AS
select serial_number, 
'watch' product_type,
if(serial_number like 'Q2%', 'Steel', 'Tintin') product_name,
if(serial_number like 'Q2%', 2.0, if(serial_number like 'Q%', 1.5, 1.0)) product_version,
if(serial_number in (select serial_number from direct_dec_2014), 'Direct',
if((serial_number in (select serial_number from temp_bestbuy_steels_asof_aug_12_2014) or serial_number in (select imei from sales_from_ingram where account_name = 'BEST BUY PURCHASING LLC')), 'Best Buy',
if( serial_number in (select imei from sales_from_ingram where account_name = 'TARGET.COM'), 'target.com',
if( serial_number in (select imei from sales_from_ingram where account_name = 'AMAZON.COM'), 'Amazon',
if( serial_number in (select imei from sales_from_ingram where account_name like 'FRY%'), 'Frys',
if( serial_number in (select imei from sales_from_ingram where account_name = 'MAGNELL ASSOCIATES INC'), 'Magnell',
if( serial_number in (select imei from sales_from_ingram where account_name = 'TARGET CORPORATION'), 'Target',
'other'))))))) sales_channel, 
mintime_phone,
maxtime_phone
from 
(select serial_number, mintime_phone, maxtime_phone, count(1)
from ztest_product_phonetimes
group by serial_number, mintime_phone, maxtime_phone) deduped