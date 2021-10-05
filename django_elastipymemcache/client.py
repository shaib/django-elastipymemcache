import logging
from distutils.version import StrictVersion

from django.utils.encoding import smart_text
from pymemcache.client.base import Client, _readline
from pymemcache.exceptions import MemcacheUnknownError

logger = logging.getLogger(__name__)


class ConfigurationEndpointClient(Client):
    # https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.AddingToYourClientLibrary.html

    def __init__(self, *args, ignore_cluster_errors=False, **kwargs):
        client = super().__init__(*args, **kwargs)
        self.ignore_cluster_errors = ignore_cluster_errors
        return client

    def _get_cluster_info_cmd(self):
        if StrictVersion(smart_text(self.version())) < StrictVersion('1.4.14'):
            return b'get AmazonElastiCache:cluster\r\n'
        return b'config get cluster\r\n'

    def _extract_cluster_info(self, line):
        raw_version, raw_nodes, _ = line.split(b'\n')
        nodes = []
        for raw_node in raw_nodes.split(b' '):
            host, ip, port = raw_node.split(b'|')
            nodes.append((smart_text(ip or host), int(port)))
        return {
            'version': int(raw_version),
            'nodes': nodes,
        }

    def _fetch_cluster_info_cmd(self, cmd, name):
        if self.sock is None:
            self._connect()
        self.sock.sendall(cmd)

        buf = b''
        result = {}
        number_of_line = 0

        while True:
            buf, line = _readline(self.sock, buf)
            self._raise_errors(line, name)
            if line == b'END':
                if number_of_line != 2:
                    raise MemcacheUnknownError('Wrong response')
                return result
            if number_of_line == 1:
                try:
                    result = self._extract_cluster_info(line)
                except ValueError:
                    raise MemcacheUnknownError('Wrong format: {line}'.format(
                        line=line,
                    ))
            number_of_line += 1

    def get_cluster_info(self):
        cmd = self._get_cluster_info_cmd()
        try:
            return self._fetch_cluster_info_cmd(cmd, 'config cluster')
        except Exception as e:
            if self.ignore_cluster_errors:
                logger.warn('Failed to get cluster: %s', e)
                return {
                    'version': None,
                    'nodes': [(
                        self.server[0],
                        int(self.server[1]),
                    )]
                }
            raise
