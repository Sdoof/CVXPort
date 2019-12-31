
import logging


Config = {
    # ---------- Paths ----------
    'data_root': 'C:/Users/Albert/Resilio Sync/FXBootcamp/Hourly',

    # ---------- Controller ----------
    'controller_http_port': 6001,
    'controller_port': 6002,  # for registration and heartbeat
    'controller_comm_port': 6003,  # for communication
    'starting_port': 6004,
    'heartbeat_interval': 10,

    # ---------- Worker ----------
    'startup_wait_time': 5,  # in seconds

    # ---------- Logging ----------
    'log_level': logging.DEBUG,
    'log_path': 'C:/Users/Albert/PycharmProjects/CVXPort/logs',
    'log_timezone': 'EST',
    'log_format': '[%(asctime)s.%(msecs)d %(levelname)s] %(message)s',
    'log_date_format': '%Y-%m-%d %H:%M:%S',

    # ---------- Data Server ----------
    'pystore_path': 'C:/Users/Albert/PycharmProjects/CVXPort/pystore',
    'subscription_wait_time': 5,

    # ---------- Interactive Brokers ----------
    'ib_port': 7497,
}
