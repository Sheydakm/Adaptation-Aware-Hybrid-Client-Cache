""""
Cache-n-DASH: A Caching Framework for DASH video streaming. 

Authors: Parikshit Juluri, Sheyda Kiyani Meher, Rohit Abhishek
Institution: University of Missouri-Kansas City
Contact Email: pjuluri@umkc.edu
    
Copyright (C) 2015, Parikshit Juluri

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""
import threading
import time
from prioritycache.cache_module import check_content_server
from prioritycache.cache_module import segment_exists
from prioritycache.prefetch_scheme import get_prefetch
import configure_cdash_log
from PriorityCache import PriorityCache
import config_cdash
import Queue
import sqlite3
#from cache_server import throughput_client

D_CONN = None
cursor_d= None

class CheckableQueue(Queue.Queue):
    """ Extending Queue to be able to check for existing values
    """
    def __contains__(self, item):
        with self.mutex:
            return item in self.queue


class CacheManager():
    def __init__(self, cache_size=config_cdash.CACHE_LIMIT):
        """ Initialize the Cache manager. 
            Start the Priority Cache with max_size = cache_size
        """
        self.fetch_requests = 0
        self.prefetch_request_count = 0
        config_cdash.LOG.info('Initializing the Cache Manager')
        self.cache = PriorityCache(cache_size)
        self.prefetch_queue = CheckableQueue()
        self.backup_prefetch_queue = CheckableQueue()
        self.current_queue = CheckableQueue()
        self.stop = threading.Event()
        self.current_thread = threading.Thread(target=self.current_function, args=())
        self.current_thread.daemon = True
        self.current_thread.start()
        config_cdash.LOG.info('Started the Current fetch thread')
        self.prefetch_thread = threading.Thread(target=self.prefetch_function, args=())
        self.prefetch_thread.daemon = True
        self.prefetch_thread.start()
        config_cdash.LOG.info('Started the Preftech thread')
        self.list_data=[]

    def terminate(self):
        self.stop.set()
        self.prefetch_thread.join()
        self.current_thread.join()

    def fetch_file(self, file_path, username=None,session_id=None):
        """ Module to get the file """
        config_cdash.LOG.info('Fetching the file {}'.format(file_path))
        n=self.backup_prefetch_queue.qsize()
        config_cdash.LOG.info('size of backup_prefetch_queue {} '.format(n))
        if n!=0:
            #item = self.backup_prefetch_queue.get()
            #config_cdash.LOG.info('item {}'.format(item))
            #config_cdash.LOG.info('size of backup_prefetch_queue {} '.format(n))
            while file_path in self.backup_prefetch_queue.queue and not segment_exists(file_path):
                config_cdash.LOG.warning('Witing untill Prefetch is complete')
                time.sleep(1)
                continue
            if file_path not in self.backup_prefetch_queue.queue and not segment_exists(file_path):
                config_cdash.LOG.warning('the request is not prefetched yet and not in the cache')

        local_filepath, http_headers = self.cache.get_file(file_path, config_cdash.FETCH_CODE)
        config_cdash.LOG.info('Added {} to current queue'.format(file_path))
        #self.current_queue.put((file_path, username, session_id))
        self.fetch_requests += 1
        config_cdash.LOG.info('Total fetch Requests = {}'.format(self.fetch_requests))
        return local_filepath, http_headers

    def current_function(self):
        """
        Thread reads the current requests and generates the prefetch requests
        that determines the next segment for all the current fetched bitrates.

        We use a separate prefetch queue to ensure that the prefetch does not affect the performance
        of the current requests
        """
        config_cdash.LOG.info('Current Thread: Started thread. Stop value = {}'.format(self.stop.is_set()))
        while not self.stop.is_set():
            try:
                current_request, username, session_id = self.current_queue.get(timeout=None)
                config_cdash.LOG.info('Retrieved the file: {}'.format(current_request))
            except Queue.Empty:
                config_cdash.LOG.error('Current Thread: Thread GET returned Empty value')
                current_request = None
                continue
            # Determining the next bitrates and adding to the prefetch list
            if current_request:
                if config_cdash.PREFETCH_SCHEME == 'SMART':
                    a=0.8
                    d=0.2
                    config_cdash.LOG.info('smart')
                    throughput_client= self.get_throughput_client(username, session_id)
                    if throughput_client== None:
                        config_cdash.LOG.info('throughput client:=None')
                        throughput= self.get_throughput_info(username, session_id, config_cdash.LIMIT, config_cdash.SCHEME)
                        config_cdash.LOG.info('average of throughput: = {}'.format(throughput))
                        forecast_throughput=throughput
                        self.insert_forecast(username, session_id,0.0)
                        self.insert_trend(username, session_id,0.0)
                        config_cdash.LOG.info('forecast throughput equal throughput:= {}'.format(forecast_throughput))
                        prefetch_request, prefetch_bitrate = get_prefetch(current_request, config_cdash.PREFETCH_SCHEME,forecast_throughput)
                    else:
                        config_cdash.LOG.info('throughput client:= {}'.format(throughput_client))
                        At_1=float(throughput_client)
                        config_cdash.LOG.info('type of At_1:= {}'.format(type(At_1)))
                        FIT_t_1=self.get_previous_forecast(username, session_id)
                        config_cdash.LOG.info('previous forecast:= {}'.format(FIT_t_1))
                        config_cdash.LOG.info('type of FIT_t_1:= {}'.format(type(FIT_t_1)))
                        Tt_1=self.get_previous_trend(username, session_id)
                        config_cdash.LOG.info('previous trend:= {}'.format(Tt_1))
                        config_cdash.LOG.info('type of Tt_1:= {}'.format(type(Tt_1)))
                        Ft=FIT_t_1+a*(At_1-FIT_t_1)
                        Tt=Tt_1+d*(Ft-FIT_t_1)
                        FIT_t=Ft+Tt
                        self.insert_forecast(username, session_id,FIT_t)
                        self.insert_trend(username, session_id,Tt)
                        config_cdash.LOG.info('forecast throughput: {}'.format(FIT_t))
                        prefetch_request, prefetch_bitrate = get_prefetch(current_request, config_cdash.PREFETCH_SCHEME,FIT_t)
                else:
                    prefetch_request, prefetch_bitrate = get_prefetch(current_request, config_cdash.PREFETCH_SCHEME, None)
                    config_cdash.LOG.info('not smart')
            if not segment_exists(prefetch_request):
                config_cdash.LOG.info('Segment not there {}'.format(prefetch_request))
                if check_content_server(prefetch_request):
                    t=type(prefetch_request)
                    config_cdash.LOG.info('Type will be saved in queue: {}'.format(t))
                    config_cdash.LOG.info('Current Thread: Current segment: {}, Next segment: {}'.format(current_request,
                                                                                                  prefetch_request))
                    self.prefetch_queue.put(prefetch_request)
                    self.backup_prefetch_queue.put(prefetch_request)
                    config_cdash.LOG.info('Pre-fetch queue count = {}'.format(self.prefetch_queue.qsize()))
                else:
                    config_cdash.LOG.info('Current Thread: Invalid Next segment: {}'.format(current_request, prefetch_request))
            else:
                config_cdash.LOG.info('Segment already there {}'.format(prefetch_request))
        else:
            config_cdash.LOG.warning('Current Thread: terminated')

    def prefetch_function(self):
        """ Function that reads the contents of the prefetch table in the database and pre-fetches the file into
            the cache
            We use a separate prefetch queue to ensure that the prefetch does not affect the performance
            of the current requests
        """
        while not self.stop.is_set():
            try:
                # Pre-fetching the files
                prefetch_request = self.prefetch_queue.get(timeout=None)
            except Queue.Empty:
                config_cdash.LOG.error('Could not read from the Pre-fetch queue')
                time.sleep(config_cdash.WAIT_TIME)
                continue
            #config_cdash.LOG.info('user {} Pre-fetching the segment: {}'.format(username,prefetch_request))
            config_cdash.LOG.info('Pre-fetching the segment: {}'.format(prefetch_request))
            self.cache.get_file(prefetch_request, config_cdash.PREFETCH_CODE)
            self.prefetch_request_count += 1
            config_cdash.LOG.info('Pre-fetch request count = {}'.format(self.prefetch_request_count))
        else:
            config_cdash.LOG.warning('Pre-fetch thread terminated')


    def get_throughput_info(self,username, session_id, limit, scheme='average'):
        last_row_tuple=self.list_data[-1]
        config_cdash.LOG.info('last row = {}'.format(last_row_tuple))
        cache_throughput=last_row_tuple[5]
        return cache_throughput


    def get_throughput_client(self,username, session_id):
        last_row_tuple=self.list_data[-1]
        config_cdash.LOG.info('last row = {}'.format(last_row_tuple))
        client_throughput=last_row_tuple[6]
        return client_throughput

    def insert_forecast(self,username, session_id,forecast):
        last_row_tuple=self.list_data[-1]
        config_cdash.LOG.info('last row = {}'.format(last_row_tuple))
        last_row=list(last_row_tuple)
        last_row[8]=forecast
        last_row_tuple=tuple(last_row)
        config_cdash.LOG.info('last row = {}'.format(last_row_tuple))
        self.list_data[-1]=last_row_tuple
        #config_cdash.LOG.info('list_data= {}'.format(self.list_data))

    def get_previous_forecast(self,username, session_id):
        previous_row_tuple=self.list_data[-2]
        config_cdash.LOG.info('previos row = {}'.format(previous_row_tuple))
        forecast=previous_row_tuple[8]
        return forecast

    def insert_trend(self,username, session_id,trend):
        last_row_tuple=self.list_data[-1]
        config_cdash.LOG.info('last row = {}'.format(last_row_tuple))
        last_row=list(last_row_tuple)
        last_row[7]=trend
        last_row_tuple=tuple(last_row)
        config_cdash.LOG.info('last row = {}'.format(last_row_tuple))
        self.list_data[-1]=last_row_tuple
        #config_cdash.LOG.info('list_data= {}'.format(self.list_data))

    def get_previous_trend(self,username, session_id):
        previous_row_tuple=self.list_data[-2]
        config_cdash.LOG.info('previous row = {}'.format(previous_row_tuple))
        trend=previous_row_tuple[7]
        return trend

