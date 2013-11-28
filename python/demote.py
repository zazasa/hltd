import os,pwd

class demote:
    def __init__(self,user):
        self.user = user
    def __call__(self):
        pw_record = pwd.getpwnam(self.user)
        os.setgid(pw_record.pw_gid)
        os.setuid(pw_record.pw_uid)
