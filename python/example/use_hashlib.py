#!/usr/local/env python3

import hashlib


def get_md5(s):
    return hashlib.md5(s.encode('utf-8')).hexdigest()


class User(object):
    def __init__(self, username, password):
        self.username = username
        #  self.salt = ''.join([chr(random.randint(48, 122)) for i in range(20)])
        self.salt = 'salt'
        self.password = get_md5(password + self.username)


db = {
    'michael': User('michael', '123456'),
    'bob': User('bob', 'abc999'),
    'alice': User('alice', 'alice2008')
}


def login(username, password):
    user = db[username]
    if user.password == get_md5(password):
        print('login successed!!!')
        return True
    else:
        print('login failed')
        return False


assert login('michael', '123456michael')
