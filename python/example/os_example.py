import os
l = [x for x in os.listdir('/') if os.path.isdir(x)]
print(l)