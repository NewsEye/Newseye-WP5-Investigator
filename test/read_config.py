import os, sys
import jwt, datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import Config


def make_token():
    return 'JWT ' + jwt.encode({"username":os.getlogin(),
                                "exp":datetime.datetime.utcnow()+datetime.timedelta(days=1)},
                               Config.SECRET_KEY,
                               algorithm="HS256").decode('utf-8')


def get_url(utility):
    if utility=="report":
        return os.path.join(Config.REPORTER_API_URI)
    return os.path.join(Config.PA_API_URI, utility+"/")


def read_config(utility="analysis"):
    return {'authorization':make_token()}, get_url(utility)



if __name__ == '__main__':
    token, url = read_config()
    print(token)
    print(url)
    
