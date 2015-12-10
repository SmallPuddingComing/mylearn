#coding:utf8

'''
Database operation module, This module is indenpendent with web module
'''
from transwarp import db

'''
设计ORM模块的原因
1、简化操作
    sql操作的是数据，关系性的数据，而python操作的是对象，为了简化变成，所以需要进行映射
    映射关系：
        数据表 ---》类
        数据表中的行 ---》实例

设计ORM接口
1、设计原则：
    根据上层调用者设计的简单用用的API
2、设计调用接口
    1.表--》类
        通过类的属性来映射表的属性（表名，字段名，字段属性）
        from transwarp.orm import Model, StringField, IntegerField
        class User():
            __table__ = 'User'
            id = IntegerField(primary_key =True)
            name = StringField()
        从中可以看出table拥有映射的表名， id是字段名，那么是字段属性名
    2.行---》实例
        通过实例的属性来映射 行的值
        #创建实例 user = User(id=123, name='michal')
        #存入数据库 user.insert()
        最后id，name变成了类的属性
'''

import time
import logging

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

#`%s`
# '--generating SQL for %s:' % table_name,
def _gen_sql(table_name, mappings):
    '''
    类 --》表时，生成创建的sql
    '''
    pk = None
    sql = ['create table %s (' % table_name]
    for f in sorted(mappings.values(), lambda x,y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError()
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(' %s %s' % (f.name, ddl) if nullable else ' %s %s not null, ' % (f.name, ddl))
    sql.append('primary key(%s)' % pk)
    sql.append(');')
    #return '\n'.join(sql)
    return ''.join(sql)

class Field(object):
    '''
    保存数据库中的表的 字段属性,在web模块中这个ORM模块是一个独立的模块
    '''
    _count = 0

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count += 1

    @property #把方法变成属性来用，例如 m.socre=100 <--> m.set_socre(100)
    def default(self):
        '''
        设置缺省值，可调用对象，比如函数
        '''
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)


class StringField(Field):
    '''
    保存String类型字段的属性
    '''
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)

class IntegerField(Field):
    '''
    保存integer类型字段的属性
    '''
    def __int__(self, **kw):
        if not 'default' in kw:
            kw['defualt'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)

class FloatField(Field):
    '''
    保存Float类型字段的属性
    '''
    def __init__(self, **kw):
        if not 'defualt' in kw:
            kw['defualt'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)

class BooleanField(Field):
    '''
    保存Boolean类型字段的属性
    '''
    def __init__(self, **kw):
        if not 'defualt' in kw:
            kw['defualt'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)

class TextField(Field):
    '''
    对text类型字段值进行追加和更新的操作
    '''
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['default'] = 'text'
        super(TextField, self).__init__(**kw)

class BlobField(Field):
    '''
    Blob是数据库用来存贮二进制文件的字段类型
    主要是用于存储图片信息用的 是以二进制的大对象，可以存贮二进制文件的容器
    '''
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)

class VersionField(Field):
    '''
    保存Version类型字段的属性
    '''
    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, defualt=0, ddl='bigint')

class ModelMetalclass(type):
    '''
    对类对象动态完成以下操作
    避免修改Model类
        1.排除对Model类的修改
    属性与字段的mapping
        1.从类的属性字典中提出 类属性和字段类 的mapping
        2.提取后 移除这些属性，防止和实例属性冲突
        3.新增‘__mapping__’属性，保存提取出来的mapping
    类和表的mapping
        1.提取类名，保存为类名，完成简单的类和表的映射
        2.新增‘__table__’属性，保存提取出来的表名
    '''

    def __new__(cls, name, bases, attrs):
        #跳过基类Model
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        #保存所有子类的信息
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class:%s' %name)

        logging.info('Scan OQMapping %s ...' %name)
        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('[MAPPING] Found mapping: %s => %s' % (k, v))

                #检查重复的主键
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than one primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE :change primary key to nou-updatable')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to nou-nullable')
                        v.nullable = False
                    primary_key = v
                mappings[k] = v

        #检查已经存在的主键
        if not primary_key:
            raise TypeError('primary not define in class %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] =None
        return type.__new__(cls, name, bases, attrs)

class Model(dict):
    '''
    这是一个基类，用户在子类中 定义映射关系，因此我们需要动态扫描子类属性，从中抽取出类属性，
    完成类《--》表的映射,,最后将扫描出结果保存成类属性
        "__table__":表名
        "__mappings__":字段对象（所有的字段属性，见Field类）
        "__primary_key__":主键字段
        "__sql__":创建sql表时执行


    >>> class User(Model):
    ...     id = IntegerField(primary_key=type)
    ...     name = StringField()
    ...     email = StringField(updatable=False)
    ...     passwd = StringField(default=lambda: '******')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.lat_modified = time.time()
    >>> u = User(id=10190, name='Michael', email='orm@db.org')
    >>> r = u.insert()
    >>> u.insert()
    'orm@db.org'
    >>> u.passwd
    '******'
    >>> u.last_modified > (time.time() - 2)
    True
    >>> f = User.get(10190)
    >>> f.name
    u'Michael'
    >>> f.email
    u'orm@db.org'
    >>> f.email = 'changed@db.org'
    >>> r = f.update() #change the email ,but email is non-updatable
    >>> len(User.find_all())
    1
    >>> g = User.get(10190)
    >>> g.email
    u'orm@db.org'
    >>> r = g.delete()
    >>> len(db.select('select * from user where id=10190'))
    0
    >>> import json
    >>> print User().__sql__()
    -- generating SQL for user:
    create table `user`(
        `id` varchar(50) not null,
        `name` varchar(255) not null.
        `email` varchar(255) not null,
        `passwd` varchar(255) not null,
        `last_modified` real not null,
        primary_key(`id`)
    );
    '''

    __metaclass__ = ModelMetalclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s" %  key)

    @classmethod
    def get(cls, pk):
        '''
        获得数据通过主键 get by primary key
        '''
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, *args):
        '''
        通过where语句查询，返回一个查询结果，如果有多个结果，仅取第一个，如果没有结果则返回第一个
        '''
        d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls, *args):
        '''
        查询所有字段，将结果以一个列表返回
        '''
        L = db.select('select * from `%s`' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, *args):
        '''
        将通过where语句但条件查询，但是返回的结果是以列表返回
        '''
        L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        '''
        执行select count(pk) from table语句，返回一个数值
        '''
        return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        '''
        执行select count(pk) from table where...语句进行查询，返回一个数值
        '''
        return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

    def update(self):
        self.pre_updata and self.pre_updata()
        L = []
        args = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.defualt
                    setattr(self, k, arg)
                L.append('`%s`=?' % k)
                args.append(arg)
        pk = self.__primary_key__.name
        args.append(getattr(self, pk))
        db.update('updata `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
        return self

    def delete(self):
        self.pre_delete and self.delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk), )
        db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)
        return self

    def insert(self):
        '''
        通过db对象的insert接口执行sql
        SQL ：insert into ‘user’ （'password'）
        '''
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        db.insert('%s' % self.__table__, **params)
        return self

# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG)
#     db.create_engine('root', '123456', 'test', '192.168.37.152')
#     db.update('drop table if exists user')
#     db.update('create table user (id int primary key ,name text, email text, passwd text, last_modified real)')
#     import doctest
#     doctest.testmod()


