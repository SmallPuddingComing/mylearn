#coding:utf8

from models import User, Blog, Comment
from transwarp import db

#开启数据库引擎，连接数据库
db.create_engine(user='root', password='123456', database='test')

#实例化一个user类，做好形成表格的准备，进行初始化Model类--》ModelMetalClass采集属性字段,然后通过Model类的函数来进行表格数据操作
u = User(name='Test', email='test@example.com', password='123456', image='about:blank', admin=False)

print User().__sql__()

#形成表格之前，判断此表格是否存在，存在下drop
db.update('drop table if exists user')

#形成表格后进行字段栏的构建,手写创建表的sql脚本
#db.update('create table user (id varchar(50) primary key, email text, password text, admin text, name text, image text, create_at float)')
#表比较多话，用Model对象通过脚本直接生成sql脚本
db.update(User().__sql__())

#字段栏形成后，进行数据的插入
u.insert()

print 'new user id:', u.id

#查询
u1 = User.find_first('where email=?', 'test@example.com')
print 'find user\'s name:', u1.name

#删除
# u1.delete()
#
# u2 = User.find_first('where email=?', 'test@example.com')
# print 'find user:', u2

