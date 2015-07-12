import logging
import sys


logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)1.1s - %(name)s: %(message)s',
                    stream=sys.stderr)
