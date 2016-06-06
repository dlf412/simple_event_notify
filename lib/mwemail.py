#!/usr/bin/env python
# encoding: utf-8

__author__ = 'deng_lingfei'


import json
import requests
from expiringdict import ExpiringDict  # expiringdict==1.1.3
from urllib import quote

# using for remove duplicate subject
mailed_cache = ExpiringDict(100, 1800)

class Mail(object):
    '''
    curl -XPOST -d "tos=tianligen@163.com&subject=TestEmail&content=测试" http://fe.falcon.ops.xxx.org:4000/sender/mail
    response is json object  {"message":"","success":true}
    '''
    @classmethod
    def set_url(cls, url):
        cls._url = url

    def __init__(self, tos, sub, content):
        if isinstance(tos, basestring):
            tos = [tos]
        elif isinstance(tos, list):
            tos = tos
        self._tos = ','.join(tos)
        self._sub = sub
        self._content = content

    def send(self):
        data = '&'.join(["tos=%s" % self._tos, "subject=%s" % quote(self._sub),
            "content=%s" % quote(self._content)])
        r = requests.post(self.url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        if r.status_code != 200:
            raise Exception("request mail service failed, code=%d, errormsg is %s" % (r.status_code, r.text))
        else:
            res = json.loads(r.text)
            if not res['success']:
                raise Exception("send mail failed, errormsg is %s" % res['message'])

def sendmail(url, tos, subject, content):
    if isinstance(tos, basestring):
        tos = [tos]
    elif isinstance(tos, list):
        tos = tos

    if url and tos and subject:
        _tos = ','.join(tos)
        data = '&'.join(["tos=%s" % _tos, "subject=%s" % quote(subject),
            "content=%s" % quote(content)])
        r = requests.post(url, data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"})

        if r.status_code != 200:
            raise Exception("request mail service failed, code=%d, errormsg is %s" % (r.status_code, r.text))
        else:
            res = json.loads(r.text)
            if not res['success']:
                raise Exception("send mail failed, errormsg is %s" % res['message'])

if __name__ == '__main__':
    Mail.url = "http://cm.ops.xxx.org/api/sendEmail"
    try:
        #Mail(["deng_lingfei@xxx.cn", "xu_xiaorong@xxx.cn"],
        #        "TestEmail", "Test0001").send()



        content = '''vdna_query -T DNA -i '/opt/xxx/media_wise/server/etc/../var/cache/20160325/1c/7d/13e4101c-f257-11e5-987d-fa163e57abbb/merge.dna' -s '192.168.1.10' -u 'zhang_jin' -w '123' -r '/opt/media_wise/server/etc/..//var/tmp/query/20160325/13e4101c-f257-11e5-987d-fa163e57abbb/192.168.1.10/receipt' -N '/opt/media_wise/server/etc/..//var/tmp/query/20160325/13e4101c-f257-11e5-987d-fa163e57abbb/192.168.1.10/crr' -C 'www.autotest.com' -b '/opt/media_wise/server/etc/..//var/tmp/query/20160325/13e4101c-f257-11e5-987d-fa163e57abbb/site' --sample_id=1516 -D '/opt/media_wise/server/etc/../var/cache/20160325/1c/7d/13e4101c-f257-11e5-987d-fa163e57abbb/192.168.1.10.debug' 2>&1None
        Server internal error'''


        sendmail(Mail.url, ["wang_aiyun@xxx.cn", "deng_lingfei@xxx.cn"], "192.168.5.218-business_router-HttpError-01010400",
                content)

        print "send mail test ok!!"

    except Exception:
        import traceback
        traceback.print_exc()

