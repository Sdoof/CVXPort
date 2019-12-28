
import pystore as ps
import pandas as pd

from cvxport import Config


# ps.set_path(Config['pystore_path'])
# store = ps.store('account')
# collection = store.collection('test')
# # collection.write('ib_ds', pd.DataFrame({'ticker': ['AAPL', 'TSLA']}), metadata={'source': 'manual'})
# df = collection.item('ib_ds')

a = {'a': 1, 'b': 3}
b = 0
for v in a.values():
    b += v
print(b)