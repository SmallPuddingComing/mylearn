#coding:utf8

'''
Create on :2015/12/3
Liense :Database operation module
'''

import time
import threading
import uuid
import functools
import logging

#Dict object
class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k,v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' Object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

def next_id(t=None):
    '''
    产生一个唯一id 当前时间+伪随机数
    @uuid :局域唯一识别码
    根据系统当前时间15位和一个随机得到的唯一码 填充3个0 补足50位的字符串
    '''
    if t is None:
        t = time.time()
    nextId = '%015d%s000' % (int(t*1000), uuid.uuid4().hex)
    return nextId

def _profiling(start, sql=''):
    '''
    用于剖析sql的执行时间
    但下划线开头的函数，不会再别的模块进行导入，只有在本模块中可以使用
    '''
    t = time.time()
    if t>0.1:
        logging.warning('[PROFILING] [DB] %s:%s',(t, sql))
    else:
        logging.info('[PROFILING] [DB] %S:%S', (t, sql))

class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass

#glabol engine object 保存着数据库的连接
engine = None

class _Engine(object):
    '''
    数据库引擎对象
    功能：用来保存create_engine创建出来的数据库链接
    '''
    def __init__(self, connect):
        self._conect = connect

    def connect(self):
        return self._conect() #这里传参为一个函数，调用函数后将结果返回

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    '''
    db的核心函数，用于连接数据库，生成全局对象engine
    engine对象持有数据库的连接
    '''
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized')#如果已经连接，表示连接重复

    #保存数据库的连接信息
    params = dict(user=user, password=password, database=database, host=host, port=port)

    #保存了数据库链接的设置、编码
    defaults = dict(use_unicode=True ,charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k,v in defaults.iteritems():
        params[k] = kw.pop(k,v)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda:mysql.connector.connect(**params))#这里返回的是一个函数

    #test connection
    logging.info('init engine <%s> is ok.' % hex(id(engine)))


#以上操作是，通过数据库引擎engine这个全局变量就可以获得一个数据库链接，重复连接会报错。

#下面是数据库的基本操作进行了封装
class _LasyConnection(object):
    '''
    惰性连接，仅当需要cursor时，才连接数据库，获取连接
    '''
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>..' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def clearup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>' % hex(id(connection)))
            connection.close()

 #以下的操作是针对不同的线程数据库链接应该是不一样的，于是创建一个变量threadlocal
class _DbCtx(threading.local):
    '''
    数据库连接的上下文对象，负责从数据库获取和释放连接，取得惰性连接对象，因此只能调用cursor对象获得正真的数据库链接
    Tread local object that holds connect info
    '''
    def __init__(self):
        self.connection = None
        self.transcations = 0

    def is_init(self):
        return self.connection is not None #判断是否已经初始化

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection()#打开了一个数据库的链接
        self.transcations = 0

    def clearup(self):
        self.connection.clearup()
        self.connection = None

    def cursor(self):
        '''return cursor'''
        return self.connection.cursor()

#由于——DbCtx是继承与threading.local，所以对每一个线程不一样的
#因此需要数据库来连接时就用它
_db_ctx = _DbCtx()
#通过_DbCtx就可以操控数据库的连接和关闭

#通过with语句，可以让数据库自动创建连接和关闭
class _ConnectionCtx(object):
    '''
    实现在_DbCtx获取和释放连接的基础上，增加了自动获取和释放连接
    '''
    def __enter__(self):
        global _db_ctx
        self.should_clearup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_clearup = True
        return self

    def __exit__(self, type, value, trace):
        global _db_ctx
        if self.should_clearup:
            _db_ctx.clearup()

def connection():
    '''
    db模块核心函数，用于获取一个数据库链接，通过_ConnectionCtx对 _db_ctx封装，使得惰性连接可以自动获取和释放
    也就是可以使用 with语法来处理数据库连接

    _ConnectionCtx 实现with语法来处理，处理自动连接
    ^
    ^
    ^
    _db_ctx  _DbCtx实例,开启线程连接数据库是不同的
    ^
    ^
    ^
    _LasyConnection 实现惰性连接
    '''
    return _ConnectionCtx()

#这里说明下with...as 的用法，with后面的语句返回_ConnectionCtx对象，然后调用这个对象的__enter__方法得到返回值，
# 返回值赋给as后面的变量，执行完毕后调用那个对象的—__exit__()

def with_connection(func):
    '''
    设计一个装饰器，用来代替频繁的with语句的操作
    '''
    def wrapper(*args, **kw):
        with connection():
            return func(*args, **kw)
    return wrapper

#下面是处理事务
class _TransactionCtx(object):
    def __init__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transcations = _db_ctx.transcations+1
        logging.info('begin transaction ...'if _db_ctx.transcations==1 else 'join current')
        return self

    def __exit__(self, type, value, trace):
        global _db_ctx
        _db_ctx.transcations = _db_ctx.transcations-1
        try:
            if _db_ctx.transcations == 0:
                if type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.clearup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('comit ok')
        except:
            logging.warning('commit fail, try rollback')
            _db_ctx.connection.rollback() #遇到故障时候，事务回滚到事务一开始的状态
            logging.warning('rollback ok')

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction ...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok....')

def transaction():
    '''
    db的核心函数 用于实现事务的功能
    '''
    return _TransactionCtx()


def with_transaction(func):
    def wrapper(*args, **kw):
        _start = time.time()
        with transaction():
            return func(*args, **kw)
        _profiling(_start)
    return wrapper


#基本操作数据库
def _select(sql, first, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL:%s ,ARGS:%s' %(sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names=[x[0] for x in cursor.description]#返回字段名的列表
        if first:
            values=cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql, *args):
    '''
    execute select sql and expected one result
    if no result found,return None
    if Multiple result found, the first one returned
    '''
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    '''
    execute select sql and expected one int and only onr int result
    '''
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column')
    return d.values()[0]

@with_connection
def select(sql, *args):
    '''
    execute select sql return list or empty list if no result
    '''
    return _select(sql, False, args)

@with_connection
def _update(sql, *args):
    '''
    use for sql operation like insert ,delet etcs
    '''
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL:%s ,ARGS:%s' %(sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transcations == 0:
            #no transaction enviroment
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def insert(table, **kw):
    '''
    exectue sql insert
    '''
    cols, args = zip(*kw.iteritems())
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    return _update(sql, *args)

def update(sql, *args):
    return _update(sql, *args)



# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG)
#     create_engine('root', '123456', 'test')
#     update('drop table if exists user')
#     update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
#     import doctest
#     doctest.testmod()
