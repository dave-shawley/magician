import logging
import os
import sys


os.environ['PYTHONASYNCIODEBUG'] = '1'
logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)1.1s - %(name)s: %(message)s',
                    stream=sys.stderr)
