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


# select
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        with (await conn.cursor(aiomysql.DictCursor)) as cur:  # 创建一个cursor 作为字典
            # 执行sql语句     SQL 语句的占位符是? mysql的占位符是%s   进行替换
            await cur.execute(sql.replace('?', '%s'), args or())
            if size:
                rs = await cur.fetchmany(size)  # 最多指定size数量的记录
            else:
                rs = await cur.fetchall()  # 返回查询集的所有结果
            logging.info('rows returnd : %s' % len(rs))
            return rs


# insert update delete      定义一个通用的execute()函数  因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数
async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            with (await conn.cursor()) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount         #返回结果数
        except BaseException as e:
            raise e
        return affected


#ORM 的基类 Model
class Model(dict,metaclass = ModelMetaclass):
