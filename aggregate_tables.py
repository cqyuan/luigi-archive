import luigi
import luigi_td

table_list = ['pebble.ztest_developers', 'pebble.ztest_product_active_hb_maxtimes',
	'pebble.ztest_product_app_hb_maxtimes', 'pebble.ztest_product_hb_times', 'pebble.ztest_product_list', 'pebble.ztest_product_phone',
	'pebble.ztest_product_phonetimes', 'pebble.ztest_product_system_hb_maxtimes', 'pebble.ztest_product_table', 'pebble.ztest_user_active_hb_maxtimes',
	'pebble.ztest_user_app_hb_maxtimes', 'pebble.ztest_user_hb_times', 'pebble.ztest_user_phone', 'pebble.ztest_user_product', 'pebble.ztest_user_system_hb_maxtimes',
	'pebble_personal_info.ztest_user_table']
#ztest_phone_list is not on this list becuase it is never run??

class DropTable(luigi_td.Query):
	type = 'presto'
	database = 'pebble'

	param = luigi.Parameter()
	source = './sql/drop_table.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/DropTables.job')

class DropAllTables(luigi.Task):
	def requires(self):
		for t in table_list:
			yield DropTable(t)

class ProductSystemHbMaxtimes(luigi_td.Query): #tested #put output into pebble
	type = 'presto'
	database = 'pebble_restricted'
	source = './sql/product_system_hb_maxtimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/ProductSystemHbMaxtimes.job')

class ProductAppHbMaxtimes(luigi_td.Query): #tested #put output into pebble
	type = 'presto'
	database = 'pebble_restricted'
	source = './sql/product_app_hb_maxtimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/ProductAppHbMaxtimes.job')


class ProductActiveHbMaxtimes(luigi_td.Query): #tested #put output into pebble
	type = 'presto'
	database = 'pebble_restricted'
	source = './sql/product_active_hb_maxtimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/ProductActiveHbMaxtimes.job')

class ProductPhonetimes(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/product_phonetimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/ProductPhonetimes.job')

class UserProduct(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/user_product.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/UserProduct.job')

class UserSystemHbMaxtimes(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble_restricted'
	source = './sql/user_system_hb_maxtimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/UserSystemHbMaxtimes.job')

class UserAppHbMaxtimes(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble_restricted'
	source = './sql/user_app_hb_maxtimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/UserAppHbMaxtimes.job')

class UserActiveHbMaxtimes(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble_restricted'
	source = './sql/user_active_hb_maxtimes.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/UserActiveHbMaxtimes.job')

class PhoneList(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/phone_list.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/PhoneList.job')

class UserPhone(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/user_phone.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/UserPhone.job')

class ProductPhone(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/product_phone.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/ProductPhone.job')

class Developers(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/developers.sql'

	def output(self):
		return luigi_td.ResultTarget('./job/Developers.job')


class ProductHbTimes(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/product_hb_times.sql'

	def requires(self):
		yield ProductActiveHbMaxtimes()
		yield ProductAppHbMaxtimes()
		yield ProductSystemHbMaxtimes()

	def output(self):
		return luigi_td.ResultTarget('./job/ProductHbTimes.job')

class UserHbTimes(luigi_td.Query): #tested
	type = 'presto'
	database = 'pebble'
	source = './sql/user_hb_times.sql'

	def requires(self):
		yield UserAppHbMaxtimes()
		yield UserActiveHbMaxtimes()
		yield UserSystemHbMaxtimes()

	def output(self):
		return luigi_td.ResultTarget('./job/UserHbTimes.job')

class ProductList(luigi_td.Query):
	type = 'presto'
	database = 'pebble'
	source = './sql/product_list.sql'

	def requires(self):
		yield ProductPhonetimes()

	def output(self):
		return luigi_td.ResultTarget('./job/ProductList.job')

class ProductTable(luigi_td.Query):
	type = 'presto'
	database = 'pebble'
	source = './sql/product_table.sql'

	def requires(self):
		yield ProductList()
		yield ProductHbTimes()
		yield UserProduct()
		yield ProductPhone()

	def output(self):
		return luigi_td.ResultTarget('./job/ProductTable.job')

class UserTable(luigi_td.Query):
	type = 'presto'
	database = 'pebble'
	source = './sql/user_table.sql'

	def requires(self):
		yield UserProduct()
		yield UserHbTimes()
		yield UserPhone()
		yield Developers()

	def output(self):
		return luigi_td.ResultTarget('./job/UserTable.job')

class AggregateTableRun(luigi.Task):
	def requires(self):
		yield UserTable()
		yield ProductTable()

if __name__ == "__main__":
	luigi.run()