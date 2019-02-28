import asyncio
import logging
import aiomysql


def log(sql, args=()):
    logging.info('SQL : %s' % sql)


# 创建连接池
async def create_pool(loop, **kw):
    logging.info('creat database connection pool ...')
    global __pool  # 连接池由全局变量__pool存储
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),  # 自动提交事务
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        ''' 创建一个字典游标  await 直接调用一个子协程 并且返回结果 cur'''
        async with (await conn.cursor(aiomysql.DictCursor)) as cur:
            # 执行sql语句     SQL 语句的占位符是? mysql的占位符是%s   进行替换
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)  # 最多指定size数量的记录
            else:
                rs = await cur.fetchall()   # 返回查询集的所有结果
            logging.info('rows returned: %s' % len(rs))
            return rs


# insert update delete
# 定义一个通用的execute()函数
# 因为这3种SQL的执行都需要相同的参数，
# 以及返回一个整数表示影响的行数   没用的行号数
async def execute(sql, args):
    log(sql)
    async with __pool.get() as conn:
        try:
            async with (await conn.cursor(aiomysql.DictCursor)) as cur:
                # execute类型的sql返回结果只有行号
                await cur.execute(sql.replace('?', '%s'), args or ())
                affected = cur.rowcount  # 返回结果数
        except BaseException as e:
            raise
        return affected



# 定义Field 类 负责保存表(数据库)的字段名和字段类型
class Field(object):
    # 表的字段包含  域名，类型，是否位主键和默认值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        # 返回   表名字  字段名  字段类型
        return '<%s, %s, %s>' % (self.__class__.__name__, self.name, self.column_type)


# 定义数据库中五个存储类型
# 字符串类型的域    varchar 可变字长
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


# 布尔类型不能作为主键
class BooleField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=False):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

# ORM 的基类 Model

# --------- 定义 Model  元类
# 所有的元类都继承自type

# ModelMetaclass 元类定义了所有Model基类(继承ModelMetaclass)的子类的实现的操作
# 元类的操作是 __new()__  在类创建的时候就会执行，而 __init()__只有在创建实例的时候才会执行 ，所以子类创建的时候就已经执行了__new()__

# ModelMetaclass 主要为一个数据库表映射成一个类封装的类做准备
# 读取具体子类(user)的映射信息
# 创建类的时候，排除对Model类的修改
# 在当前类中查找所有的类的属性(attrs),如果找到Field 属性，就将其保存到__mappings__的dict中 同时从类属性中删除Field(防止实例属性遮住类的同名属性)
# 将数据库表名保存到__table__中


class ModelMetaclass(type):
    # cls ：当前准备创建的类的对象；
    # name ：类的名字；
    # bases：类继承的父类集合；
    # attrs: 类的方法集合。dict类型
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身  因为要排除对model类的修改
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table的名称    数据库对应的表名 如果不存在那么就是等于类名
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table:%s)' % (name, tableName))
        # 获取所有的Field和主键名
        # 创建映射字典
        mappings = dict()
        # 域list
        fields = []
        # 主键标记
        primaryKey = None
        # k 表示字段名
        # 获取类中的所有的键值对
        for k, v in attrs.items():
            # 选择Field类型实例的属性作为映射键值
            if isinstance(v, Field):
                logging.info(' found mapping : %s ==> %s' % (k, v))
                # 将当前的键值对放入mapping中
                mappings[k] = v
                if v.primary_key:
                    # 防止出现两个、两个以上的主键
                    # 找到主键  当第一次主键存在primaryKey被赋值 后来如果再出现主键的话就会引发错误
                    if primaryKey:
                        raise RuntimeError(
                            'Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    # 将key添加进入fields中，也就是映射类中的属性和数据库中的表的域, 这里面不包含主键
                    fields.append(k)
        # 前面可能没有找到主键，提示一下
        if not primaryKey:
            raise RuntimeError('primary key is not found')
        # 从类属性中删除Field(防止实例属性遮住类的同名属性)  所有的Field已经保存到了mappings中了
        for k in mappings.keys():
            attrs.pop(k)

        # map(f,b)  两个参数    一个函数，一个可迭代序列，将序列中的每一个元素作为函数参数进行运算加工，
        # 将list  fields中的每个元素按照匿名函数加工        ` `
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName      # 表名
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (
            primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(
            escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(
            map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (
            tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)        # 调用type生成类


# 继承自ModelMetaclass元类、dict的类
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作


# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射

class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 重写get方法
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

   # 重写set方法
    def __setattr__(self, key, value):
        self[key] = value

    # 重写get方法
    def getValue(self, key):
        return getattr(self, key, None)

   # 获取值，当不存在的时候获取的是默认值
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' %
                              (key, str(value)))
                setattr(self, key, value)
        return value

    # 增加类的方法 通过@classmethod 修饰
    # 定义类的方法有三种，
    # 1: 普通定义               需要通过self参数隐式的传递当前类对象的实例。    绑定对象的实例
    # 2：@classmethod 定义      需要通过cls参数传递当前类对象。                绑定对象
    # 3：@staticmethod定义      定义与普通函数是一样的                         没有参数绑定。

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        # 获取元类自动生成的SQL语句,并根据当前的参数，继续合成
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('orderBy')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?,?')
                args.append(limit)
            else:
                raise ValueError('invalid limit value: %s' % str(limit))
        # 直接调用select函数来处理, 这里是等待函数执行完成函数才能返回
        rs = await select(''.join(sql), args)
        # 该类本身是字典，自己用自己生成新的实例，里面的阈值正好也是需要查询   不懂
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        'find number by select and where'
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # 增加Model类的实例方法，所有子类都可以调用实例方法
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affectd rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning(
                'failed to update by primary key : affectd rows ：%s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delet__, args)
        if rows != 1:
            logging.warning(
                'failed to remove by primary key : affectd rows : %s' % rows)
