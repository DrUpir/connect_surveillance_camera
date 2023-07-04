import cv2
import multiprocessing
import os
import shutil
import time
from datetime import datetime, timedelta
from typing import Final

import pypylon.pylon as py
from django.core.cache import cache

from camera.config import create_parquet, upload
from backend.settings import BASE_DIR


def create_parquet_and_send_hfds(folder_in, folder_out, tr, username, password):
    create_parquet(folder_in, folder_out)
    upload(folder_out, tr, username, password)
    shutil.rmtree(folder_in)
    shutil.rmtree(folder_out)


class FrameCollector():
    """Class for working with Basler camera frames."""

    # IP is 10.50.221.164 for Basler acA1920-40gm (22039069).
    _FIRST_CAMERA_NAME: Final = 'Basler acA1920-40gm (22039069)'
    # Camera timeout in ms.
    _CAMERA_TIMEOUT: Final = 1000


    def __init__(self, dev_no: int):
        self.dev_no = dev_no
        self.dev_id = dev_no + 1
        self.dataset_path = os.path.join(BASE_DIR, 'media', 'archive')
        self.parquet_path = os.path.join(BASE_DIR, 'media', 'parquet')
        self.image_path = os.path.join(self.dataset_path, f'cam{self.dev_id}')

        # Get instance of the pylon TransportLayerFactory.
        tlf = py.TlFactory.GetInstance()
        dev_info_set = tlf.EnumerateDevices()
        if dev_info_set[0].GetFriendlyName() != self._FIRST_CAMERA_NAME:
            dev_info_set = dev_info_set[::-1]
        # The active camera will be an InstantCamera based on a device
        #  created with the corresponding DeviceInfo.
        self.cam = py.InstantCamera(tlf.CreateDevice(dev_info_set[dev_no]))
        self.cam.Open()
        self.cam.StartGrabbing(py.GrabStrategy_LatestImageOnly)
        
        self.datetime_check_cache = datetime.now() - timedelta(minutes=2)
        self.datetime_record_dataset = None
        self.url = None
        self.username = None 
        self.user_password = None
        self.status_save_dataset = False
    
    def run(self):
        """Start collecting frames."""
        while True:
            datetime_now = datetime.now()
            if self.datetime_check_cache + timedelta(minutes=1) < datetime_now:
                self.status_save_dataset = True
                save_dataset = cache.get('save_dataset')
                if (
                    save_dataset 
                    and self.url is None 
                    and self.username is None
                    and self.user_password is None
                ):
                    try:
                        datetime_record_dataset_str = save_dataset['time']
                        self.url = save_dataset['url']
                        self.username = save_dataset['username']
                        self.user_password = save_dataset['user_password']
                        self.datetime_record_dataset = datetime.now() + timedelta(minutes=int(datetime_record_dataset_str))
                        print(f"datetime_record_dataset = {self.datetime_record_dataset}")
                        print(f"datetime.now() = {datetime.now()}")
                        os.makedirs(self.image_path, exist_ok=True)
                    except Exception as check_cache_exception:
                        print(f'Не удалось начать запись датасета. Причина {check_cache_exception}')
                self.datetime_check_cache = datetime.now()

            with self.cam.RetrieveResult(self._CAMERA_TIMEOUT) as res:
                if res.GrabSucceeded():
                    img = res.Array
                    retval, buffer = cv2.imencode(".jpg", img)
                    name = f"cam{self.cam_id}_bytes"
                    cache.set(name, buffer.tobytes(), 60)
                    if self.datetime_record_dataset and datetime.now() < self.datetime_record_dataset:
                        cv2.imwrite(os.path.join(self.image_path, f'{time.time()}.png'), img)
                    if self.status_save_dataset:
                        save_path_dataset = os.path.join(BASE_DIR, 'media', 'dataset', f'{self.cam_id}',
                                                        f"{datetime_now.strftime('%d_%m_%Y')}")
                        os.makedirs(save_path_dataset, exist_ok=True)
                        img_path = os.path.join(save_path_dataset, f"{datetime.now().strftime('%d_%m_%Y_%H_%M_%S_%f')}.jpg")
                        cv2.imwrite(img_path, img)
                        self.status_save_dataset = False
                else:
                    # Re-initiate object in unsuccessful image grabbing case.
                    self.__init__(dev_no=self.dev_no)

            if self.datetime_record_dataset and datetime.now() > self.datetime_record_dataset:
                if int(self.cam_no) == 0:
                    p1 = multiprocessing.Process(target=create_parquet_and_send_hfds,
                                                args=(self.dataset_path,
                                                    self.parquet_path,
                                                    self.url,
                                                    self.username,
                                                    self.user_password))
                    p1.start()
                self.datetime_record_dataset = None
                self.url = None
                self.username = None 
                self.user_password = None

        self.cam.StopGrabbing()
        self.cam.Close()


def start_getting_frames(cam_no):
    """Start getting frames."""
    FrameCollector(cam_no).run()
