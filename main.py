
from cvxport.utils import get_prices
from cvxport import config

if __name__ == '__main__':
    data_root = config['data_root']
    data = get_prices(['EURUSD', 'GBPUSD'], data_root)


