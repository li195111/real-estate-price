import argparse
from turtle import ht

def option():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dotenv', type=bool, default=True,
                        help='if True, will load .env file for config.')
    parser.add_argument('--download_path', type=str, default='./downloads',
                        help="crawler's download file to save.")
    parser.add_argument('--zip_file_name', type=str, default='download.zip',
                        help="crawler's download file name.")
    parser.add_argument('--unzip_dir_name', type=str, default='data',
                        help="crawler's download file unzip to dist directory name.")
    parser.add_argument('--publish', type=str, default='108年第2季',
                        help="Publish season name.")
    parser.add_argument('--file_format', type=str, default='csv',
                        help="crawler's download file format.")
    parser.add_argument('--names', nargs='+', default=['臺北市','新北市','桃園市','臺中市','高雄市'],
                        help="crawler's download file city names.")
    parser.add_argument('--name_codes', nargs='+', default=['A','F','H','B','E'],
                        help="crawler's download file city code.")
    parser.add_argument('--category', type=str, default='不動產資料',
                        help="crawler's download file category. options=['不動產資料', '預售屋買賣', '不動產租賃']")
    parser.add_argument('--disable-redis', type=bool, default=True,
                        help='Disable the redis.')
    parser.add_argument('--api',  action='store_true',
                        help='if set, will run run.py and execute fastapi api server.')
    return parser.parse_args()