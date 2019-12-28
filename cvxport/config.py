
import logging


Config = {
    # ---------- Paths ----------
    'data_root': 'C:/Users/Albert/Resilio Sync/FXBootcamp/Hourly',

    # ---------- Controller ----------
    'controller_http_port': 6001,
    'controller_port': 6002,
    'starting_port': 6003,
    'heartbeat_interval': 10,

    # ---------- Controller ----------
    'startup_wait_time': 5,  # in seconds

    # ---------- Logging ----------
    'log_level': logging.DEBUG,
    'log_path': 'C:/Users/Albert/PycharmProjects/CVXPort/logs',
    'log_timezone': 'EST',
    'log_format': '[%(asctime)s.%(msecs)d %(levelname)s] %(message)s',
    'log_date_format': '%Y-%m-%d %H:%M:%S',

    # ---------- Data ----------
    'pystore_path': 'C:/Users/Albert/PycharmProjects/CVXPort/pystore',
}
