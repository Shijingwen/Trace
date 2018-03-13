# -*- coding: utf-8 -*-
# !usr/bin/python3
"""
    Ali Trace Machine Statistics
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    copyright:(c) 2017 by Jingwen Shi
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class TraceStatistics(object):

    def __init__(self):
        # Add data path
        path_server_event = os.path.join("./data/server_event.csv")
        path_container_event = os.path.join("./data/container_event.csv")
        path_batch_instance = os.path.join("./data/batch_instance.csv")

        path_batch_task = os.path.join("./data/batch_task.csv")
        path_container_usage = os.path.join("./data/container_usage.csv")
        path_server_usage = os.path.join("./data/server_usage.csv")
        """
        # Load distribution map
        col_batch = ['time_istart', 'time_iend', 'jid', 'tid', 'mid', 'status''num_sqe',
                     'num_sqe_total', 'num_cpu_maxus', 'num_cpu_aveus', 'norm_mem_maxu', 'norm_mem_aveus']
        col_container = ['timestampc', 'etypec', 'insid', 'mid',
                         'num_cpu_req', 'norm_mem_req', 'norm_disk_req', 'id_cpu_allo']
        col_server = ['timestamps', 'mid', 'etypes',
                      'edetail','num_cpu', 'norm_mem', 'norm_disk']
        self.batch_instance = pd.DataFrame(pd.read_csv(path_batch_instance,
                                           names=col_batch, index_col=False))# 16094655 rows
        self.container_event = pd.DataFrame(pd.read_csv(path_container_event,
                                            names=col_container, index_col=False))# 11102 rows
        self.server_event = pd.DataFrame(pd.read_csv(path_server_event,
                                         names=col_server, index_col=False))# 1352 rows
        """
        # Load usage data
        col_btask = ['time_tcreate', 'time_tend', 'jid', 'tid',
                     'num_ins', 'status', 'num_cpu_req', 'norm_mem_req']
        col_cusage = ['time_start','insid_online', 'per_cpu_req',
                      'per_mem_req', 'per_disk_req', 'ave1_cpu', 'ave5_cpu',
                      'ave15_cpu', 'ave_cpi', 'ave1000_mem_miss', 'max_cpi', 'max_mem_miss']
        col_susage = ['timestampsu', 'mid', 'per_cpu', 'per_mem',
                      'per_disk', 'ave1_cpu', 'ave5_cpu', 'ave15_cpu']
        #self.batch_task = pd.DataFrame(pd.read_csv(path_batch_task,
        #                              names=col_btask, index_col=False))
        self.container_usage = pd.DataFrame(pd.read_csv(path_container_usage,
                                            names=col_cusage, index_col=False))
        self.server_usage = pd.DataFrame(pd.read_csv(path_server_usage,
                                         names=col_susage, index_col=False))

    def usage_hour(self):
        # usgae min/max/median of server
        copy_serv = self.server_usage
        grouped_scpu_usage = copy_serv['per_cpu'].groupby([copy_serv['timestampsu']])
        count_scpu_usage = grouped_scpu_usage.agg(['min', 'max', 'median'])
        count_scpu_usage.to_csv('./scpu_usage')
        grouped_smem_usage = copy_serv['per_mem'].groupby([copy_serv['timestampsu']])
        count_smem_usage = grouped_smem_usage.agg(['min', 'max', 'median'])
        count_smem_usage.to_csv('./smem_usage')
        grouped_sdisk_usage = copy_serv['per_disk'].groupby([copy_serv['timestampsu']])
        count_sdisk_usage = grouped_sdisk_usage.agg(['min', 'max', 'median'])
        count_sdisk_usage.to_csv('./sdisk_usage')

        # Usage min/max/median of container
        copy_con = self.container_usage
        grouped_ccpu_usage = copy_con['per_cpu_req'].groupby([copy_con['time_start']])
        count_ccpu_usage = grouped_ccpu_usage.agg(['min', 'max', 'median'])
        count_ccpu_usage.to_csv('./ccpu_usage')
        grouped_cmem_usage = copy_con['per_mem_req'].groupby([copy_con['time_start']])
        count_cmem_usage = grouped_cmem_usage.agg(['min', 'max', 'median'])
        count_cmem_usage.to_csv('./cmem_usage')
        copy_con = self.container_usage
        grouped_cdisk_usage = copy_con['per_disk_req'].groupby([copy_con['time_start']])
        count_cdisk_usage = grouped_cdisk_usage.agg(['min', 'max', 'median'])
        count_cdisk_usage.to_csv('./cdisk_usage')

    def draw_usage(self):
        # Load datas of server usage
        col_serv = ['timestampsu', 'min', 'max', 'median']
        scpu_usage = pd.DataFrame(pd.read_csv('./scpu_usage', names=col_serv, index_col=False))
        smem_usage = pd.DataFrame(pd.read_csv('./smem_usage', names=col_serv, index_col=False))
        sdisk_usage = pd.DataFrame(pd.read_csv('./sdisk_usage', names=col_serv, index_col=False))
        print(type(scpu_usage['timestampsu'].get_values()))
        # Draw plots of server usage
        fig1 = plt.figure(1)
        plt.subplot(131)
        plt.plot(scpu_usage['timestampsu'].get_values(), scpu_usage['min'].get_values())
        """
        ax11 = fig1.add_subplot(1, 3, 1)
        ax11.plot(scpu_usage['timestampsu'], scpu_usage['min'])
        ax12 = fig1.add_subplot(1, 3, 2)
        ax12.plot(smem_usage['timestampsu'], scpu_usage['max'])
        ax13 = fig1.add_subplot(1, 3, 3)
        ax13.plot(sdisk_usage['timestampsu'], scpu_usage['median'])

        # Load datas of container usage
        col_con = ['time_start', 'min', 'max', 'median']
        ccpu_usage = pd.DataFrame(pd.read_csv('./ccpu_usage', names=col_con, index_col=False))
        cmem_usage = pd.DataFrame(pd.read_csv('./cmem_usage', names=col_con, index_col=False))
        cdisk_usage = pd.DataFrame(pd.read_csv('./cdisk_usage', names=col_con, index_col=False))

        # Draw plots of container usage
        fig2 = plt.figure(2)
        ax21 = fig2.add_subplot(1, 3, 1)
        ax21.plot(ccpu_usage['time_start'], ccpu_usage['min'])
        ax22 = fig2.add_subplot(1, 3, 2)
        ax22.plot(cmem_usage['time_start'], ccpu_usage['max'])
        ax23 = fig2.add_subplot(1, 3, 3)
        ax23.plot(cdisk_usage['time_start'], ccpu_usage['median'])
        """
        plt.show()

    def conf_type(self):
        """
        Count machine configurations using groupby()
        Cpu = 0 memory = 0 disk = 0  are not counted
        """
        copy = self.server_event
        copy['one'] = 1
        conf = copy[self.server_event.num_cpu > 0]
        grouped_conf = conf['one'].groupby([conf['num_cpu'], conf['norm_mem'],
                                            conf['norm_disk']])
        conf_type = grouped_conf.count()
        conf_type.to_csv('./conf_type.csv', sep='|')
        # List mid of different configuration and check if there're duplicated machines
        grouped_conf0 = conf['one'].groupby([conf['num_cpu'], conf['norm_mem'],
                                            conf['norm_disk'], conf['mid']])
        conf_type0 = grouped_conf0.count()
        conf_type0.to_csv('./conf_type0.csv', sep='|')
        # Output results
        print("Total Number of machine:")
        print(len(set(self.server_event['mid'])))
        print("Configuration types:")
        print(conf_type)

    def job_duration(self):
        # Run time of task
        time_tcreate = pd.DataFrame(self.batch_task['time_tcreate'],
                                    self.batch_task['jid']).groupby('jid')
        min_tcreate = time_tcreate.min()
        time_tend = pd.DataFrame(self.batch_task['time_tend'],
                                 self.batch_task['jid']).groupby('jid')
        max_tend = time_tend.max()
        create_stop = pd.merge(min_tcreate, max_tend, on='jid')
        duration = [create_stop['jid'], create_stop['time_tend']-create_stop['time_tcreate']]
        print(duration)

    def task_requested_resource(self):
        # Raw Required resources (CPU and Memory) of task
        max_cpu = self.batch_task['num_cpu_req'].max()
        norm_cm_req = np.array(self.batch_task['jid'], self.batch_task['num_cpu_req']/max_cpu,
                               self.batch_task['norm_mem_req'])
        print(norm_cm_req)

mstatistic = TraceStatistics()
mstatistic.conf_type()
#mstatistic.usage_hour()
#mstatistic.draw_usage()
