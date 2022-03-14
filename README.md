# real-estate-price - Etanits Interview
Python Taiwan Real Estate Price API Server & Crawler

# Requirements
- OS: Windows 10 or 11 (Test Pass: Windows 11)
- Python: 3.7
- [Chrome Driver](https://chromedriver.chromium.org/)
- [Spark 3.0.3](https://spark.apache.org/downloads.html)
- Hadoop 3.2
- (Optional) [Redis](https://redis.io/download)
- (Optional) Miniconda
# How To Start

1. Setup Python 3.7, Spark and Hadoop environments and run following command.
```
git clone https://github.com/li195111/real-estate-price.git
cd real-estate-price
pip install -r requirements.txt
```
2. Rename the `.env-smaple` file. to `.env` and adjust the value whatever you want.
3. if you are already download the `Chrome Driver` and configured the DRIVER_PATH in `.env` then run main.py will download the specific real-estate-price data from https://plvr.land.moi.gov.tw/DownloadOpenData

```
python main.py
```

Default will setup the config by load the `.env` file.

If you want set by command can run following.
```
python main.py --dotenv=False --download_path=./downloads --publish=108年第2季
```
Run API server after main process. add `--api` flag
```
python main.py --api --dotenv=False --download_path=./downloads --publish=108年第2季
```

## Server Only

You can also run the api server only by

```
python run.py
```

## Disable Redis
Create a `.env` file and set the following environment variable

```
DISABLE_REDIS=True # default: True
```
or 
```
python main.py --disable-redis=True --api
```

## API Documents

Portal - http://localhost:3000

with Params - http://localhost:3000/search?your-param=param-value

|Method | URL | Params | Description |
|-|-|-|-|
|GET| /search |
| | | main_purpose | 主要用途 |
| | | town | 鄉鎮市區 |
| | | floor_ls | 小於 總樓層數 |
| | | floor_eq | 等於 總樓層數 |
| | | floor_gt | 大於 總樓層數 |
| | | btye | 建物型態 |

### Example

`http://localhost:3000/search?town=中壢區&floor_gt=10&btye=住宅大樓`

Result:
```
{
  "status": "success",
  "time": "2022-03-14 04:41:18.2022-03-14",
  "msg": "success",
  "result": {
    "桃園市": {
      "2019-04-29": [
        {
          "district": "中壢區",
          "building_state": "住宅大樓(11層含以上有電梯)",
          "number_of_floor": 16
        }
      ],
      "2019-04-25": [
        {
          "district": "中壢區",
          "building_state": "住宅大樓(11層含以上有電梯)",
          "number_of_floor": 15
        },
        ...
```
