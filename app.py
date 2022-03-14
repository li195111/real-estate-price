import glob
import json
import os
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from jwcrypto import jwe, jwk, jws, jwt

if not os.environ.get('DISABLE_REDIS',False) == 'True':
    import redis
    rdb = redis.Redis(host='localhost',port=6379,db=0)
    
from exceptions import InvalidTokenException, TokenNotExistsException
from main import SparkSession, df2result, get_spark_df, taiwandate_num2str
from utils import error_msg, utc_now

default_map = {'category_map':{'不動產資料':'A','預售屋買賣':'B','不動產租賃':'C'}}

try:
    from main import name_set, unzip_dir
except ImportError:
    from dotenv import load_dotenv
    load_dotenv()
    download_path = os.environ.get('download_path','./downloads')
    download_path = os.path.abspath(download_path)
    unzip_dir_name = os.environ.get('unzip_dir_name','datas')
    unzip_dir = os.path.join(download_path,unzip_dir_name)
    names = os.environ.get('names','臺北市,新北市,桃園市,臺中市,高雄市').split(',')
    name_codes = os.environ.get('name_codes','A,F,H,B,E').split(',')
    default_map['code_name_map'] = {code:name for code, name in zip(name_codes,names)}
    category = os.environ.get('category','不動產資料')
    category_map = default_map['category_map']
    name_set = {f'{code}_lvr_land_{category_map[category]}'.lower() for code in name_codes}

app = FastAPI()
origins = [
    "http://localhost:3001"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    default_map['jwt_alg'] = 'RSA-OAEP-256'
    default_map['jwt_enc'] = 'A256CBC-HS512'
    default_map['jwt_kty'] = 'RSA'
    default_map['jwt_size'] = 2048
    default_map['jwt_kid'] = uuid.uuid4().hex
    default_map['jwt_key'] = jwk.JWK.generate(kty=default_map['jwt_kty'],size=default_map['jwt_size'], kid=default_map['jwt_kid'])
    default_map['jwt_header'] = {'alg':default_map['jwt_alg'],'enc':default_map['jwt_enc']}
    default_map['load_file_paths'] = [file for file in glob.glob(rf'{unzip_dir}/*.csv', recursive=True) if os.path.basename(file).split('.')[0].lower() in name_set]
    # .config("spark.sql.execution.arrow.pyspark.enabled","true") # for use toPandas
    default_map['spark'] = SparkSession.builder\
        .appName("realEstatePriceSession")\
        .getOrCreate()
    default_map['union_df'] = get_spark_df(default_map['spark'], default_map['load_file_paths'], default_map['code_name_map'])
    print (default_map['union_df'].count())

@app.on_event("shutdown")
def shutdown_event():
    default_map['spark'].stop()

# Exception Handler
@app.exception_handler(AssertionError)
async def http_exception_handler(request: Request, exc: AssertionError) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(IndexError)
async def http_exception_handler(request: Request, exc: IndexError) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(AttributeError)
async def http_exception_handler(request: Request, exc: AttributeError) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(jwe.InvalidJWEData)
async def http_exception_handler(request: Request, exc: jwe.InvalidJWEData) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(jws.InvalidJWSSignature)
async def http_exception_handler(request: Request, exc: jwe.InvalidJWEData) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(TokenNotExistsException)
async def http_exception_handler(request: Request, exc: TokenNotExistsException) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(InvalidTokenException)
async def http_exception_handler(request: Request, exc: InvalidTokenException) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).part_message}, status_code=403)

@app.exception_handler(Exception)
async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse({'status':'failed','time':utc_now(),'msg':error_msg(exc).details_message}, status_code=404)

# API
# ---------------------------------------------------
# GET /
# ---------------------------------------------------
@app.get('/')
async def index(req:Request) -> JSONResponse:
    return JSONResponse({'status':'success','time':utc_now(),"msg":"success"})

# ---------------------------------------------------
# GET /authentication
# ---------------------------------------------------
@app.get('/authentication')
async def authentication(req:Request) -> JSONResponse:
    valid_time = 3
    expiry_time = round(time.time()) + valid_time
    claims = {'exp':expiry_time}
    token = jwt.JWT(header=default_map['jwt_header'], claims=claims)
    token.make_encrypted_token(default_map['jwt_key'])
    return JSONResponse({'status':'success','time':utc_now(), "msg":"success",'token':token.serialize(),'pbk':default_map['jwt_key'].export_public()} )

# ---------------------------------------------------
# POST /authentication
# ---------------------------------------------------
@app.post('/authentication')
async def authentication(req:Request) -> JSONResponse:
    body_bytes = await req.body()
    body = body_bytes.decode()
    assert body != '', 'no data received.'
    json_data = json.loads(body)
    data = json_data.get('data')
    token_str = json_data.get('token')
    if token_str is None: raise TokenNotExistsException('Token No Found')
    token = jwt.JWT()
    try:
        token.deserialize(token_str, default_map['jwt_key'])
    except ValueError: raise InvalidTokenException("Invalid Token Found")
    if data is None: raise ValueError('no data received.')
    data_split = data.split('@')
    if len(data_split) > 0:
        jwe_token = jwe.JWE()
        jwe_token.deserialize(data_split[1], default_map['jwt_key'])
        payload = jwe_token.payload.decode('utf-8')
        assert not payload is None, "No payload received"
        secret_infos = json.loads(payload)
        # Some Verify User
        verify = (secret_infos['username'] == 'abc12345') and (secret_infos['password'] == '1234567890')
        if verify:
            valid_time = 60 * 60 * 24 * 1 # 24 hrs
            expiry_time = round(time.time()) + valid_time
            claims = {'exp':expiry_time}
            token = jwt.JWT(header=default_map['jwt_header'], claims=claims)
            token.make_encrypted_token(default_map['jwt_key'])
            return JSONResponse({'status':'success','time':utc_now(), "msg":"success", 'token':token.serialize()})
        return JSONResponse({'status':'failed','time':utc_now(), 'msg':'invalid user'})
    return JSONResponse({'status':'failed','time':utc_now(), 'msg':'no data received.'})
    
# Task 3.
# ---------------------------------------------------
# GET /search
# ---------------------------------------------------
@app.get('/search')
async def search(req:Request) -> JSONResponse:
    # # For authentication user
    # json_data = json.loads((await req.body()).decode())
    # token_str = json_data.get('token')
    # if token_str is None: raise TokenNotExistsError('Token No Found')
    # token = jwt.JWT()
    # try:
    #     token.deserialize(token_str, default_map['jwt_key'])
    # except ValueError:
    #         raise InvalidTokenException("Invalid Token Found")
    query = req.query_params
    main_purpose = query.get('main')
    town = query.get('town')
    floor_ls = query.get('floor_ls')
    floor_eq = query.get('floor_eq')
    floor_gt = query.get('floor_gt')
    btye = query.get('btye')
    res = default_map['union_df']
    if not main_purpose is None:
        res = res.where(default_map['union_df']['主要用途'] == main_purpose)
    if not town is None:
        res = res.where(default_map['union_df']['鄉鎮市區'] == town)
    if not floor_ls is None:
        res = res.where(default_map['union_df']['總樓層數N'] <= floor_ls)
    if not floor_eq is None:
        res = res.where(default_map['union_df']['總樓層數N'] == floor_eq)
    if not floor_gt is None:
        res = res.where(default_map['union_df']['總樓層數N'] >= floor_gt)
    if not btye is None:
        res = res.where(default_map['union_df']['建物型態'].contains(btye))
    result = df2result(res,['district','building_state','number_of_floor'],['鄉鎮市區','建物型態','總樓層數N'])
    return JSONResponse({'status':'success','time':utc_now(), "msg":"success", 'result':result})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run('main:app', host='0.0.0.0', port=3000, reload=True, log_level='debug')

