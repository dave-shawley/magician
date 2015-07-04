"""Errors detected and reported by this package."""


class ProtocolFailure(Exception):
    """
    Raised when a protocol violation is detected.

    Coerce instances to a string for a useful message.

    :param str message_format: format to pass to :meth:`str.format`
        along with `*args` and `**kwargs` to generate the stored
        message
    :param args: positional parameters for :meth:`str.format`
    :param kwargs: keyword parameters for :meth:`str.format`

    """

    def __init__(self, message_format, *args, **kwargs):
        self._message = message_format.format(*args, **kwargs)
        super(ProtocolFailure, self).__init__(self._message)

    def __str__(self):
        return '{0}: {1}'.format(self.__class__.__name_, self._message)
