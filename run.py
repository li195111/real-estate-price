from dotenv import load_dotenv
import uvicorn

from app import app

original_callback = uvicorn.main.callback

def callback(**kwargs):
    from celery.contrib.testing.worker import start_worker

    import tasks

    with start_worker(tasks.task, perform_ping_check=False, loglevel="info"):
        original_callback(**kwargs)

if __name__ == "__main__":
    import os
    import sys
    import threading
    import multiprocessing
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-file', type=str, default='./.env',
                        help='Environ variable file path.')
    opts = parser.parse_args()
    env_exists = os.path.exists(opts.env_file)
    if env_exists:
        load_dotenv(opts.env_file)
    if not os.environ.get('DISABLE_REDIS',False) == 'True':
        uvicorn.main.callback = callback

        # Execute redis-server
        redis = threading.Thread(target=os.system,args=('redis-server',)) 
        redis.start()
        # Execute Celery Schedule
        celery_schedule = multiprocessing.Process(target=os.system, args=('celery -A tasks beat',))
        celery_schedule.start()

    # frontend = multiprocessing.Process(target=os.system,args=('cd auth-front-end && npm run dev',)) 
    # frontend.start()
    
    # uvicorn filename:app 
    sys.argv = sys.argv[0:1]+['app:app','--reload','--host=0.0.0.0','--port=3000']
    if env_exists:
        sys.argv += [f'--env-file={opts.env_file}']
    uvicorn.main()
    
    if opts.redis:
        redis.join()
        celery_schedule.join()
    # frontend.join()
    