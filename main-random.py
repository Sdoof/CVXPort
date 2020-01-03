
from cvxport.data import Asset
import json

a = {Asset('FX:EURUSD'): 1}
msg = json.dumps(a)
print(msg)