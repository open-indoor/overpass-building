#!/usr/bin/env python3

# Check new data every minutes, and check older data otherwise

import requests
import time
import traceback
from multiprocessing import Process
import atexit

MAX_CONCURRENT_JOB = 5

class BuildingUpdateMinutely:

    building_diff_url='https://pikachu.openindoor.io'
    auth=('openindoor', 'pcd/7+r3SZJ1UX/QEH+72tIH')

    def __init__(self):
        # self.data = []
        self.last_seq_num = self.get_last_seq_num()
        atexit.register(self.cleanup)

    def cleanup(self):
        oldest_seq_num_file = open('./data/oldest_seq_num.txt', 'w+')
        oldest_seq_num_file.write(str(self._past_seq_num))
        oldest_seq_num_file.close()
        latest_seq_num_file = open('./data/latest_seq_num.txt', 'w+')
        latest_seq_num_file.write(str(self.last_seq_num))
        oldest_seq_num_file.close()

    @staticmethod
    def get_last_seq_num():
        try:
            req = BuildingUpdateMinutely.building_diff_url + '/get-seq-number/minutely'
            print('req:', req)
            last_seq_num_request = requests.get(req)
        except requests.exceptions.RequestException as err:
            traceback.print_exc()
            time.sleep(3)
            return 0
        if last_seq_num_request.status_code >= 400:
            traceback.print_exc()
            time.sleep(3)
            return 0
        last_seq_num_json = last_seq_num_request.json()
        return last_seq_num_json['sequence_number']

    def update_seq(self, seq_num):
        url = BuildingUpdateMinutely.building_diff_url + '/update/minutely/' + str(seq_num) + '/' + str(MAX_CONCURRENT_JOB)
        print(time.strftime("%Y-%m-%d %H:%M:%S"), '- url:', url)
        try:
            update_seq_request = requests.get(
                url,
                auth=BuildingUpdateMinutely.auth,
                timeout=300
            )
            if update_seq_request.status_code >= 400:
                self.trace_seq_issues(seq_num)
                return None
            update_seq_json = update_seq_request.json()
            print(time.strftime("%Y-%m-%d %H:%M:%S"), '- update_seq:', update_seq_json)
            return update_seq_json
        except requests.exceptions.RequestException as err:
            traceback.print_exc()
            self.trace_seq_issues(seq_num)
            time.sleep(3)
            return {'success': False}

    def trace_seq_issues(self, seq_num):
        with open('./data/sequence_issues.txt', 'a+') as sequence_id_file:
            sequence_id_file.write(str(seq_num) + "\n")

    def start(self, past_seq_num, latest_seq_num):
        self._past_seq_num = past_seq_num
        print('past_seq_num:', past_seq_num)
        self.last_seq_num = latest_seq_num if (latest_seq_num is not None) else 0
        end_of_file = False
        with open('./data/footprint_update.txt', 'r+') as building_id_file:
            # print('building_line:', next(building_id_file))
            # building_lines = building_id_file.readlines()

            # _past_seq_num = past_seq_num
            while True:
                last_seq_num = self.get_last_seq_num()
                # Last sequence number has changed => update last diff
                if self.last_seq_num != last_seq_num:
                    seq_nums = range(self.last_seq_num, last_seq_num)
                    print(time.strftime("%Y-%m-%d %H:%M:%S"), '- seq_nums:', str(seq_nums))
                    for seq_num in seq_nums:
                        # print('Update last seq num:', seq_num)
                        print('********** Update latest seq num:', str(seq_num), '**********')
                        self.update_seq(seq_num)
                    self.last_seq_num = last_seq_num
                # No change => update others
                else:
                    # 1 - Update specific buildings
                    # for i in range(10):
                    #     # No more than 5 // jobs
                    #     print('********** (', str(i+1), '/ 10 ) Update', str(MAX_CONCURRENT_JOB), 'buildings concurrently **********')
                    #     jobs=[]
                    #     for i in range(MAX_CONCURRENT_JOB):
                    #         if not end_of_file:
                    #             try:
                    #                 building_id = next(building_id_file).rstrip("\n")
                    #                 building_type='wr'
                    #                 match building_id[0]:
                    #                     case 'w':
                    #                         building_type='way'
                    #                     case 'a':
                    #                         building_type='relation'
                    #                 building_id_ = building_id[1:]
                    #                 # print('Update building:', building_id)
                    #                 url = 'https://songoku.openindoor.io/api/building/update/' + building_type + '/' + building_id_
                    #                 print('url:', url)
                    #                 job = Process(target=requests.get, args=(url,))
                    #                 jobs.append(job)
                    #                 job.start()
                    #                 # requests.get(url)
                    #             except StopIteration:
                    #                 end_of_file = True
                    #         else:
                    #             break
                    #     for job in jobs:
                    #         job.join()
                    # 2 - Update from past seq num
                    if self._past_seq_num <= 0:
                        continue
                    print('********** Update past seq num:', str(self._past_seq_num), '**********')
                    self.update_seq(self._past_seq_num)
                    self._past_seq_num -= 1

if __name__ == '__main__':
    print('BuildingUpdateMinutely')
    last_seq_num = BuildingUpdateMinutely.get_last_seq_num()
    print('last_seq_num:', last_seq_num)
    latest_seq_num = 0
    # with open('./data/latest_seq_num.txt', 'r+') as latest_seq_num_file:
    #     try:
    #         latest_seq_num = int(next(latest_seq_num_file).rstrip("\n"))
    #     except StopIteration:
    #         latest_seq_num = 0
    oldest_seq_num = 0
    # with open('./data/oldest_seq_num.txt', 'r+') as oldest_seq_num_file:
    #     try:
    #         oldest_seq_num = int(next(oldest_seq_num_file).rstrip("\n"))
    #     except StopIteration:
    #         oldest_seq_num = 0
    past_seq_num = oldest_seq_num if (oldest_seq_num != 0) else last_seq_num - 1
    latest_seq_num = latest_seq_num if (latest_seq_num != 0) else last_seq_num
    building_update_minutely = BuildingUpdateMinutely()
    building_update_minutely.start(
        past_seq_num = past_seq_num,
        latest_seq_num = latest_seq_num,
    )