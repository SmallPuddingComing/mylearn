#coding:utf8

'''
构建一个简单轻量级，WSGI兼容的web框架
WSGI概要：
    方式： WSGI server ---> WSGI 处理函数
    功能：将http院士请求、解析、响应 这些交给WSGI server完成，
        我们专心用python编写web业务，也就是WSGI的处理函数
        所以WSGI是http的一种高级封装
    例子：
        wsgi 处理函数
            def application(environ, start_response):
                method = environ['REQUEST_MEYHOD']
                path = environ['PATH']
                if method == 'GET' and path=='/':
                    return handle_home(environ, start_response)
                if method == 'POST' and path=='/signin':
                   return handle_signin(environ, start_response)
        wigi server
            def run(self, port=9000, host='127.0.0.1'):
                from wsgiref.simple_server import make_server
                server = make_server(host, port, application)
                server .serve_forever()

设计web框架的原因：
    1、Wsgi提供的接口虽然比HTTP接口高级了不少，但是和web App的处理逻辑比，还是比较低级，
        我们需要在WAGI接口上进一步抽象，让我们专注于用一个函数处理URL，至于URL的函数映射，就让web框架来处理。

设计web框架接口：
    1、URL路由：用于URL到 处理函数的映射
    2、URL拦截：用于根据URL做权限检测
    3、视图：用于HTML页面生成
    4、数据模型：用于抽取数据（models模块）
    5、事务数据：request数据和response数据的封装（thread local）

'''

import types, os, re, cgi, sys, time, datetime, functools, mimetypes, threading, logging, traceback, urllib

from db import Dict
import utils

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

#实现事务数据接口，实现request数据和response数据的存贮
#是一个全局Threadlocal对象
ctx = threading.local()

_RE_RESPONSE_STATUS = re.compile(r'^\d\d\d(\[\w\ ]+)?$')
_HEADER_X_POWERED_BY = ('X-Powered-By', 'transwarp/1.0')

#用于时区转换
_TIMEDELTA_ZERO = datetime.timedelta(0) #0:00:00
_RE_TZ = re.compile('^([\+\-])([0-9]{1,2})\:([0-9]{1,2})$')

#response status
_RESPONSE_STATUSES = {
    # Informational
    100: 'Continue',
    101: 'Switching Protocols',
    102: 'Processing',

    #Sucessful
    200: 'Ok',
    201: 'Created',
    202: 'Accepted',
    203: 'Non-Authoritative Information',
    204: 'No Content',
    205: 'Reset Content',
    206: 'Partial Content',
    207: 'Multi Status',
    226: 'IM Used',

    # Redirection
    300: 'Multiple Choices',
    301: 'Moved Permanently',
    302: 'Found',
    303: 'See Other',
    304: 'Not Modified',
    305: 'Use Proxy',
    307: 'Temporary Redirect',

    # Client Error
    400: 'Bad Request',
    401: 'Unauthorized',
    402: 'Payment Required',
    403: 'Forbidden',
    404: 'Not Found',
    405: 'Method Not Allowed',
    406: 'Not Acceptable',
    407: 'Proxy Authentication Required',
    408: 'Request Timeout',
    409: 'Conflict',
    410: 'Gone',
    411: 'Length Required',
    412: 'Precondition Failed',
    413: 'Request Entity Too Large',
    414: 'Request URI Too Long',
    415: 'Unsupported Media Type',
    416: 'Requested Range Not Satisfiable',
    417: 'Expectation Failed',
    418: "I'm a teapot",
    422: 'Unprocessable Entity',
    423: 'Locked',
    424: 'Failed Dependency',
    426: 'Upgrade Required',

    # Server Error
    500: 'Internal Server Error',
    501: 'Not Implemented',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout',
    505: 'HTTP Version Not Supported',
    507: 'Insufficient Storage',
    510: 'Not Extended',
}

_RESPONSE_HEADERS = (
    'Accept-Ranges',
    'Age',
    'Allow',
    'Cache-Control',
    'Connection',
    'Content-Encoding',
    'Content-Language',
    'Content-Length',
    'Content-Location',
    'Content-MD5',
    'Content-Disposition',
    'Content-Range',
    'Content-Type',
    'Date',
    'ETag',
    'Expires',
    'Last-Modified',
    'Link',
    'Location',
    'P3P',
    'Pragma',
    'Proxy-Authenticate',
    'Refresh',
    'Retry-After',
    'Server',
    'Set-Cookie',
    'Strict-Transport-Security',
    'Trailer',
    'Transfer-Encoding',
    'Vary',
    'Via',
    'Warning',
    'WWW-Authenticate',
    'X-Frame-Options',
    'X-XSS-Protection',
    'X-Content-Type-Options',
    'X-Forwarded-Proto',
    'X-Powered-By',
    'X-UA-Compatible',
)

class UTC(datetime.tzinfo):
    '''
    tzinfo 是一个抽象基类，用于给datetime对象分配一个时区，
    使用的方式，把这个子类对象传递给datetime.tzinfo属性
    传递方式：
    1、在初始化的时候：
        datetime(2009,2,17,19,10,2,tzinfo=tz0)
    2、使用datetime对象的replace方法传入，从新生成一个datetime
        datetime.replace(tzinfo=tz0)
    '''

    def __init__(self, utc):
        utc = str(utc.strip().upper())
        mt = _RE_TZ.match(utc)
        if mt:
            minus = mt.group(1) == '-'
            h = int(mt.group(2))
            m = int(mt.group(3))
            if minus:
                h, m = (-h), (-m)
            self._utcoffset = datetime.timedelta(hours=h, minutes=m)
            self._tzname = 'UTC%s' % utc
        else:
            raise ValueError('bad utc time zone')

    def utcoffset(self, date_time):
        '''
        表示与标准时区的偏移量
        '''
        return self._utcoffset

    def dst(self, date_time):
        '''
        夏令时
        '''
        return self._tzname

    def tzname(self, date_time):
        '''
        所在区时的名字
        '''
        return self._tzname

    def __str__(self):
        return 'UTC timezone info object (%s)' % self._tzname

    __repr__ = __str__

UTC_0 = UTC('+00:00')

#用于异常处理
class _HttpError(Exception):
    '''
    HttpError that define http error code

    >>>e = _HttpError(404)
    >>>e.status
    >>>'404 Not Found'
    '''

    def __init__(self, code):
        '''
        初始化异常处理的代号
        '''
        super(_HttpError, self).__init__()
        self.status = '%d %s', (code, _RESPONSE_STATUSES[code])
        self._headers = None

    def header(self, name, value):
        '''
        查看有没有头信息，没有的话就添加powererd by header
        '''
        if not self._headers:
            self._headers = [_HEADER_X_POWERED_BY]
        self._headers.append((name, value))

    @property
    def headers(self):
        """
        使用setter方法实现的 header属性
        """
        if hasattr(self, '_headers'):
            return self._headers
        return []

    def __str__(self):
        return self.status

    __repr__ = __str__

class _RedirectError(_HttpError):
    pass

