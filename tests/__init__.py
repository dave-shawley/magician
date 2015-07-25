import logging
import os
import sys


os.environ['PYTHONASYNCIODEBUG'] = '1'
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
                    format=('%(relativeCreated)-8d %(levelname)1.1s - '
                            '%(name)s: %(message)s (%(filename)s:%(lineno)d)'))
