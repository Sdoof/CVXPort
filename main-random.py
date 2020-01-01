
import pandas as pd
from datetime import datetime
import cProfile
import time

from cvxport import const
from cvxport.data import DownSampledBar, Datum, Asset


bar = DownSampledBar(const.Freq.MINUTE, const.Freq.SECOND5)
now = pd.Timestamp.now()
delta = pd.Timedelta(seconds=5)

pr = cProfile.Profile()
pr.enable()

for i in range(100000):
    data = Datum(Asset('FX:EURUSD'), now + i * delta, 1, 2, 3, 4)
    bar.update(data)

pr.disable()
pr.print_stats(sort='calls')