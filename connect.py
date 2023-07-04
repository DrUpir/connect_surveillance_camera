import os
import cv2
import time
import shutil
import multiprocessing
import pypylon.pylon as py

from datetime import datetime, timedelta
from camera.config import create_parquet, upload
from backend.settings import BASE_DIR
from django.core.cache import cache



def create_parquet_and_send_hfds(folder_in, folder_out, tr, username, password):
    create_parquet(folder_in, folder_out)
    upload(folder_out, tr, username, password)
    shutil.rmtree(folder_in)
    shutil.rmtree(folder_out)

def start_getting_frames(cam_no):
    cam_id = cam_no + 1
    path_for_save_dataset = os.path.join(BASE_DIR, 'media', 'archive')
    path_for_save_parquet = os.path.join(BASE_DIR, 'media', 'parquet')
    path_for_save_image = os.path.join(path_for_save_dataset, f'cam{cam_id}')

    tlf = py.TlFactory.GetInstance()
    di = py.DeviceInfo()

    cams = tlf.EnumerateDevices([di, ])

    if cams[0].GetFriendlyName() != 'Basler acA1920-40gm (22039069)':
        cams = cams[::-1]

    # Basler acA1920-40gm (22039069) = ip 10.50.221.164


    cam = py.InstantCamera(tlf.CreateDevice(cams[cam_no]))

    cam.Open()
    cam.StartGrabbing(py.GrabStrategy_LatestImageOnly)
    datetime_check_cache = datetime.now() - timedelta(minutes=2)
    datetime_record_dataset = url = username = user_password = None
    status_save_dataset = False

    while 1:
        datetime_now = datetime.now()
        if datetime_check_cache + timedelta(minutes=1) < datetime_now:
            status_save_dataset = True
            if cache.get('save_dataset') and all(x is None for x in [url, username, user_password]):
                try:
                    data = cache.get('save_dataset')
                    datetime_record_dataset_str = data['time']
                    url = data['url']
                    username = data['username']
                    user_password = data['user_password']
                    datetime_record_dataset = datetime.now() + timedelta(minutes=int(datetime_record_dataset_str))
                    print(f"datetime_record_dataset = {datetime_record_dataset}")
                    print(f"datetime.now() = {datetime.now()}")
                    os.makedirs(path_for_save_image, exist_ok=True)
                except Exception as check_cache_exception:
                    print(f'Не удалось начать запись датасета. Причина {check_cache_exception}')
            datetime_check_cache = datetime.now()

        with cam.RetrieveResult(1000) as res:
            if res.GrabSucceeded():
                img = res.Array
                retval, buffer = cv2.imencode(".jpg", img)
                name = f"cam{cam_id}_bytes"
                cache.set(f"{name}", buffer.tobytes(), 60)
                if datetime_record_dataset and datetime.now() < datetime_record_dataset:
                    cv2.imwrite(os.path.join(path_for_save_image, f'{time.time()}.png'), img)
                if status_save_dataset:
                    save_path_dataset = os.path.join(BASE_DIR, 'media', 'dataset', f'{cam_id}',
                                                     f"{datetime_now.strftime('%d_%m_%Y')}")
                    os.makedirs(save_path_dataset, exist_ok=True)
                    img_path = os.path.join(save_path_dataset, f"{datetime.now().strftime('%d_%m_%Y_%H_%M_%S_%f')}.jpg")
                    cv2.imwrite(img_path, img)
                    status_save_dataset = False
        if datetime_record_dataset and datetime.now() > datetime_record_dataset:
            if int(cam_no) == 0:
                p1 = multiprocessing.Process(target=create_parquet_and_send_hfds,
                                             args=(path_for_save_dataset,
                                                   path_for_save_parquet,
                                                   url,
                                                   username,
                                                   user_password))
                p1.start()
            url = username = user_password = datetime_record_dataset = None
    cam.StopGrabbing()
    cam.Close()